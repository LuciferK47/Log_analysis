"""
ingestion.py — ArduPilot DataFlash Log Ingestion Layer

Reads .bin DataFlash logs using pymavlink's DFReader (NOT mavutil which is
for telemetry .tlog streams). Dynamically determines which message types to
extract by parsing feature_registry.yaml, rather than relying on a hardcoded
list.
"""
import re
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Regex to find column references like ATT.Roll, RCOU.C1, NKF4.SP
_COL_REF = re.compile(r'(?:\$\{)?([A-Z][A-Z0-9]+)\.[A-Za-z0-9_]+(?:\})?')


def extract_msg_types_from_registry(config_path: str) -> list[str]:
    """
    Parse feature_registry.yaml and extract the unique message type prefixes
    (e.g., ATT, RCOU, NKF4) referenced in priority_1 and fallback fields.
    This makes ingestion fully declarative — adding a new feature to the YAML
    automatically causes its message type to be ingested.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    prefixes = set()
    for feat_name, rules in config.get('features', {}).items():
        # Extract from priority_1
        p1 = rules.get('priority_1', '')
        if p1:
            match = re.match(r'([A-Z][A-Z0-9]+)\.', p1)
            if match:
                prefixes.add(match.group(1))

        # Extract from fallback expression
        fallback = rules.get('fallback', '')
        if fallback:
            for match in _COL_REF.finditer(fallback):
                prefixes.add(match.group(1))

    result = sorted(prefixes)
    logger.info("Dynamically resolved message types from registry: %s", result)
    return result


class LogReader:
    """Reads ArduPilot DataFlash .bin logs into a unified pandas DataFrame."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_and_resample(self, target_hz: int = 10,
                          msg_types: list = None,
                          config_path: str = None,
                          generate_dummy: str = None) -> pd.DataFrame:
        """
        Parse a .bin log, extract message streams, and resample to a
        unified frequency.

        Args:
            target_hz: Target resampling frequency in Hz (default: 10).
            msg_types: Explicit list of message type strings to extract.
                       If None and config_path is provided, types are
                       dynamically resolved from the feature registry.
            config_path: Path to feature_registry.yaml for dynamic type
                         resolution.
            generate_dummy: If set, skip real parsing and generate a
                            synthetic fault scenario. One of:
                            'motor_loss', 'gps_glitch', 'vibration', None.

        Returns:
            pd.DataFrame indexed by timedelta with columns like 'ATT.Roll',
            'RCOU.C1', etc.
        """
        if generate_dummy:
            return self._generate_dummy_data(scenario=generate_dummy)

        # Dynamically resolve message types from the YAML registry
        if msg_types is None and config_path:
            msg_types = extract_msg_types_from_registry(config_path)
        elif msg_types is None:
            # Absolute fallback if no config provided
            msg_types = ['ATT', 'RCOU', 'NKF4', 'GPS', 'VIBE', 'BATT',
                         'CTUN', 'MAG']

        return self._parse_bin(msg_types, target_hz)

    # ------------------------------------------------------------------ #
    #  Real .bin parsing via DFReader                                      #
    # ------------------------------------------------------------------ #
    def _parse_bin(self, msg_types: list, target_hz: int) -> pd.DataFrame:
        """Parse a real DataFlash .bin log file."""
        try:
            from pymavlink import DFReader
        except ImportError:
            raise ImportError(
                "pymavlink is required. Install with: pip install pymavlink"
            )

        logger.info("Reading %s via DFReader...", self.filepath)

        # DFReader_binary handles the FMT self-describing binary format
        log = DFReader.DFReader_binary(self.filepath, zero_time_base=True)

        # Collect rows keyed by message type
        streams: dict[str, list[dict]] = {t: [] for t in msg_types}

        while True:
            msg = log.recv_msg()
            if msg is None:
                break

            mtype = msg.get_type()
            if mtype not in streams:
                continue

            row = {'TimeUS': msg.TimeUS}
            # Dynamically discover fields from the FMT definition
            fieldnames = msg.get_fieldnames()
            for field in fieldnames:
                if field == 'TimeUS':
                    continue
                try:
                    row[f"{mtype}.{field}"] = getattr(msg, field)
                except AttributeError:
                    pass
            streams[mtype].append(row)

        # Build per-type DataFrames, then merge on a common time axis
        dfs = []
        for mtype, rows in streams.items():
            if not rows:
                logger.warning("No %s messages found in log.", mtype)
                continue
            tdf = pd.DataFrame(rows)
            tdf['TimeUS'] = pd.to_timedelta(tdf['TimeUS'], unit='us')
            tdf = tdf.set_index('TimeUS')
            dfs.append(tdf)

        if not dfs:
            logger.error("No relevant messages found in %s", self.filepath)
            return pd.DataFrame()

        # Merge all streams on the time axis, then resample
        merged = pd.concat(dfs, axis=1)

        period_ms = 1000 // target_hz
        merged = merged.resample(f'{period_ms}ms').first()
        merged = merged.ffill().dropna(how='all')

        logger.info(
            "Parsed %d rows across %d columns at %d Hz.",
            len(merged), len(merged.columns), target_hz,
        )
        return merged

    # ------------------------------------------------------------------ #
    #  Dummy data generators for testing without SITL                      #
    # ------------------------------------------------------------------ #
    def _generate_dummy_data(self, scenario: str = 'motor_loss') -> pd.DataFrame:
        """
        Generate synthetic telemetry to test the pipeline without real logs.
        Scenarios mirror the SITL fault-injection commands from the proposal.
        """
        generators = {
            'motor_loss': self._dummy_motor_loss,
            'gps_glitch': self._dummy_gps_glitch,
            'vibration': self._dummy_vibration,
        }
        gen = generators.get(scenario)
        if gen is None:
            raise ValueError(
                f"Unknown dummy scenario '{scenario}'. "
                f"Choose from: {list(generators.keys())}"
            )
        logger.info("Generating dummy '%s' scenario...", scenario)
        return gen()

    def _dummy_motor_loss(self) -> pd.DataFrame:
        """
        Simulates SIM_ENGINE_FAIL=1: Motor 1 dies mid-flight.
        Roll diverges, RCOU.C1 saturates compensating.
        """
        n = 200  # 20 seconds at 10Hz
        t = pd.timedelta_range(start='0s', periods=n, freq='100ms')
        df = pd.DataFrame(index=t)

        fault_start = 100  # Fault at t=10s

        # --- ATT ---
        df['ATT.DesRoll'] = 0.0
        df['ATT.DesPitch'] = 0.0
        roll = np.zeros(n)
        roll[fault_start:] = np.linspace(0, 45, n - fault_start)
        df['ATT.Roll'] = roll
        df['ATT.Pitch'] = np.random.normal(0, 0.5, n)

        # --- RCOU (4 motors) ---
        df['RCOU.C1'] = 1500.0
        df['RCOU.C2'] = 1500.0
        df['RCOU.C3'] = 1500.0
        df['RCOU.C4'] = 1500.0
        # Motor 1 saturates trying to compensate
        df.loc[t[fault_start]:, 'RCOU.C1'] = np.linspace(1500, 2000,
                                                           n - fault_start)
        # Motor 3 (opposite) drops
        df.loc[t[fault_start]:, 'RCOU.C3'] = np.linspace(1500, 1100,
                                                           n - fault_start)

        # --- VIBE (normal) ---
        df['VIBE.VibeX'] = np.random.normal(5, 1, n)
        df['VIBE.VibeY'] = np.random.normal(5, 1, n)
        df['VIBE.VibeZ'] = np.random.normal(8, 1, n)
        df['VIBE.Clip0'] = 0.0

        # --- BATT ---
        df['BATT.Volt'] = np.linspace(16.8, 15.2, n)
        df['BATT.Curr'] = np.random.normal(12, 1, n)

        # --- GPS (normal) ---
        df['GPS.HDop'] = np.random.normal(0.8, 0.1, n).clip(0.5)
        df['GPS.NSats'] = 14.0

        # --- NKF4 (normal) ---
        df['NKF4.SP'] = np.random.normal(0.3, 0.05, n).clip(0.1)

        return df

    def _dummy_gps_glitch(self) -> pd.DataFrame:
        """
        Simulates SIM_GPS_GLITCH: HDop spikes and NKF4.SP exceeds 1.0.
        """
        n = 200
        t = pd.timedelta_range(start='0s', periods=n, freq='100ms')
        df = pd.DataFrame(index=t)
        fault_start = 80

        df['ATT.Roll'] = np.random.normal(0, 1, n)
        df['ATT.DesRoll'] = 0.0
        df['ATT.Pitch'] = np.random.normal(0, 1, n)
        df['ATT.DesPitch'] = 0.0

        df['RCOU.C1'] = 1500.0
        df['RCOU.C2'] = 1500.0
        df['RCOU.C3'] = 1500.0
        df['RCOU.C4'] = 1500.0

        # GPS glitch: HDop spikes, sat count drops
        hdop = np.full(n, 0.8)
        hdop[fault_start:fault_start + 40] = np.linspace(0.8, 5.0, 40)
        df['GPS.HDop'] = hdop
        nsats = np.full(n, 14.0)
        nsats[fault_start:fault_start + 40] = np.linspace(14, 4, 40)
        df['GPS.NSats'] = nsats

        # EKF position innovation spikes
        sp = np.full(n, 0.3)
        sp[fault_start:fault_start + 40] = np.linspace(0.3, 2.5, 40)
        df['NKF4.SP'] = sp

        df['VIBE.VibeX'] = np.random.normal(5, 1, n)
        df['VIBE.Clip0'] = 0.0
        df['BATT.Volt'] = 16.0

        return df

    def _dummy_vibration(self) -> pd.DataFrame:
        """
        Simulates SIM_VIB_MOT_MAX=30: Excessive motor vibration with
        IMU clipping.
        """
        n = 200
        t = pd.timedelta_range(start='0s', periods=n, freq='100ms')
        df = pd.DataFrame(index=t)
        fault_start = 60

        df['ATT.Roll'] = np.random.normal(0, 1, n)
        df['ATT.DesRoll'] = 0.0

        df['RCOU.C1'] = 1500.0
        df['RCOU.C2'] = 1500.0
        df['RCOU.C3'] = 1500.0
        df['RCOU.C4'] = 1500.0

        # Vibration spikes
        vx = np.random.normal(5, 1, n)
        vx[fault_start:] = np.random.normal(35, 5, n - fault_start)
        df['VIBE.VibeX'] = vx
        df['VIBE.VibeY'] = vx * 0.8
        df['VIBE.VibeZ'] = vx * 1.2

        # Clipping events
        clip = np.zeros(n)
        clip[fault_start:] = np.cumsum(
            np.random.poisson(3, n - fault_start)
        ).astype(float)
        df['VIBE.Clip0'] = clip

        df['GPS.HDop'] = 0.8
        df['GPS.NSats'] = 14.0
        df['BATT.Volt'] = 16.0
        df['NKF4.SP'] = np.random.normal(0.3, 0.05, n).clip(0.1)

        return df
