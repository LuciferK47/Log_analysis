"""
ingestion.py — ArduPilot DataFlash Log Ingestion Layer

Reads .bin DataFlash logs using pymavlink's DFReader (NOT mavutil which is
for telemetry .tlog streams). Dynamically determines which message types AND
which specific fields to extract by parsing feature_registry.yaml, so only
the data that's actually needed is loaded into memory.

For a 30-minute real log, this means extracting ~6 columns instead of ~200,
reducing RAM from gigabytes to megabytes.
"""
import re
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Regex to find column references like ATT.Roll, RCOU.C1, NKF4.SP
_COL_REF = re.compile(r'(?:\$\{)?([A-Z][A-Z0-9]+\.[A-Za-z0-9_]+)(?:\})?')
_MSG_PREFIX = re.compile(r'^([A-Z][A-Z0-9]+)\.')


def parse_required_columns(config_path: str) -> dict[str, set[str]]:
    """
    Parse feature_registry.yaml and extract which specific columns are
    needed from which message types.

    Returns:
        dict mapping message type → set of field names.
        e.g. {'ATT': {'Roll', 'DesRoll', 'ErrRP'}, 'RCOU': {'C1','C2',...}}
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    needed: dict[str, set[str]] = {}

    for feat_name, rules in config.get('features', {}).items():
        # Extract from priority_1
        p1 = rules.get('priority_1', '')
        if p1:
            _add_column(p1, needed)

        # Extract from fallback expression
        fallback = rules.get('fallback', '')
        if fallback:
            for match in _COL_REF.finditer(fallback):
                _add_column(match.group(1), needed)

    logger.info("Registry requires %d message types: %s",
                len(needed), sorted(needed.keys()))
    for msg, fields in sorted(needed.items()):
        logger.debug("  %s: %s", msg, sorted(fields))

    return needed


def _add_column(col_ref: str, needed: dict[str, set[str]]):
    """Parse 'ATT.Roll' into needed['ATT'].add('Roll')."""
    m = _MSG_PREFIX.match(col_ref)
    if m:
        msg_type = m.group(1)
        field = col_ref[len(msg_type) + 1:]
        needed.setdefault(msg_type, set()).add(field)


class LogReader:
    """Reads ArduPilot DataFlash .bin logs into a unified pandas DataFrame."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_and_resample(self, target_hz: int = 10,
                          config_path: str = None,
                          generate_dummy: str = None) -> pd.DataFrame:
        """
        Parse a .bin log, extract ONLY the message types and fields
        specified in the feature registry, and resample to a unified
        frequency.

        Args:
            target_hz: Target resampling frequency in Hz (default: 10).
            config_path: Path to feature_registry.yaml for dynamic column
                         resolution. Required for real .bin parsing.
            generate_dummy: If set, skip real parsing and generate a
                            synthetic fault scenario.

        Returns:
            pd.DataFrame indexed by timedelta with columns like 'ATT.Roll',
            'RCOU.C1', etc.
        """
        if generate_dummy:
            return self._generate_dummy_data(scenario=generate_dummy)

        if config_path is None:
            raise ValueError(
                "config_path (feature_registry.yaml) is required for "
                "parsing real .bin logs."
            )

        needed = parse_required_columns(config_path)
        return self._parse_bin(needed, target_hz)

    # ------------------------------------------------------------------ #
    #  Real .bin parsing via DFReader                                      #
    # ------------------------------------------------------------------ #
    def _parse_bin(self, needed: dict[str, set[str]],
                   target_hz: int) -> pd.DataFrame:
        """
        Parse a real DataFlash .bin log file, extracting only the
        message types and fields specified in `needed`.
        """
        try:
            from pymavlink import DFReader
        except ImportError:
            raise ImportError(
                "pymavlink is required. Install with: pip install pymavlink"
            )

        logger.info("Reading %s via DFReader...", self.filepath)
        log = DFReader.DFReader_binary(self.filepath, zero_time_base=True)

        msg_types = set(needed.keys())
        streams: dict[str, list[dict]] = {t: [] for t in msg_types}

        while True:
            msg = log.recv_msg()
            if msg is None:
                break

            mtype = msg.get_type()
            if mtype not in msg_types:
                continue

            # Only extract the fields we actually need (memory-efficient)
            row = {'TimeUS': msg.TimeUS}
            wanted_fields = needed[mtype]
            for field in wanted_fields:
                try:
                    row[f"{mtype}.{field}"] = getattr(msg, field)
                except AttributeError:
                    # Field doesn't exist in this firmware version;
                    # the abstraction layer's fallback will handle it.
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

        merged = pd.concat(dfs, axis=1)

        period_ms = 1000 // target_hz
        merged = merged.resample(f'{period_ms}ms').first()
        merged = merged.ffill().dropna(how='all')

        logger.info(
            "Parsed %d rows across %d columns at %d Hz "
            "(only requested fields loaded).",
            len(merged), len(merged.columns), target_hz,
        )
        return merged

    # ------------------------------------------------------------------ #
    #  Dummy data generators for testing without SITL                      #
    # ------------------------------------------------------------------ #
    def _generate_dummy_data(self, scenario: str = 'motor_loss') -> pd.DataFrame:
        generators = {
            'motor_loss': self._dummy_motor_loss,
            'gps_glitch': self._dummy_gps_glitch,
            'vibration': self._dummy_vibration,
        }
        gen = generators.get(scenario)
        if gen is None:
            raise ValueError(
                f"Unknown scenario '{scenario}'. "
                f"Choose from: {list(generators.keys())}"
            )
        logger.info("Generating dummy '%s' scenario...", scenario)
        return gen()

    def _dummy_motor_loss(self) -> pd.DataFrame:
        n = 200  # 20 seconds at 10Hz
        t = pd.timedelta_range(start='0s', periods=n, freq='100ms')
        df = pd.DataFrame(index=t)
        fault_start = 100  # 10s

        df['ATT.DesRoll'] = 0.0
        df['ATT.DesPitch'] = 0.0
        roll = np.zeros(n)
        roll[fault_start:] = np.linspace(0, 45, n - fault_start)
        df['ATT.Roll'] = roll
        df['ATT.Pitch'] = np.random.normal(0, 0.5, n)

        df['RCOU.C1'] = 1500.0
        df['RCOU.C2'] = 1500.0
        df['RCOU.C3'] = 1500.0
        df['RCOU.C4'] = 1500.0
        df.loc[t[fault_start]:, 'RCOU.C1'] = np.linspace(1500, 2000,
                                                           n - fault_start)
        df.loc[t[fault_start]:, 'RCOU.C3'] = np.linspace(1500, 1100,
                                                           n - fault_start)

        df['VIBE.VibeX'] = np.random.normal(5, 1, n)
        df['VIBE.VibeY'] = np.random.normal(5, 1, n)
        df['VIBE.VibeZ'] = np.random.normal(8, 1, n)
        df['VIBE.Clip0'] = 0.0
        df['BATT.Volt'] = np.linspace(16.8, 15.2, n)
        df['BATT.Curr'] = np.random.normal(12, 1, n)
        df['GPS.HDop'] = np.random.normal(0.8, 0.1, n).clip(0.5)
        df['GPS.NSats'] = 14.0
        df['NKF4.SP'] = np.random.normal(0.3, 0.05, n).clip(0.1)
        return df

    def _dummy_gps_glitch(self) -> pd.DataFrame:
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

        hdop = np.full(n, 0.8)
        hdop[fault_start:fault_start + 40] = np.linspace(0.8, 5.0, 40)
        df['GPS.HDop'] = hdop
        nsats = np.full(n, 14.0)
        nsats[fault_start:fault_start + 40] = np.linspace(14, 4, 40)
        df['GPS.NSats'] = nsats

        sp = np.full(n, 0.3)
        sp[fault_start:fault_start + 40] = np.linspace(0.3, 2.5, 40)
        df['NKF4.SP'] = sp

        df['VIBE.VibeX'] = np.random.normal(5, 1, n)
        df['VIBE.Clip0'] = 0.0
        df['BATT.Volt'] = 16.0
        return df

    def _dummy_vibration(self) -> pd.DataFrame:
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

        vx = np.random.normal(5, 1, n)
        vx[fault_start:] = np.random.normal(35, 5, n - fault_start)
        df['VIBE.VibeX'] = vx
        df['VIBE.VibeY'] = vx * 0.8
        df['VIBE.VibeZ'] = vx * 1.2

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
