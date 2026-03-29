"""
ingestion.py — ArduPilot DataFlash Log Ingestion Layer

Reads .bin DataFlash logs using pymavlink's DFReader. Dynamically determines
which message types and fields to extract from feature_registry.yaml.

Domain-aware features:
  - Parses PARM table for vehicle configuration (FRAME_CLASS, FRAME_TYPE)
  - Extracts MODE messages and forward-fills across the time-series
  - Collects MSG (text warnings) and ERR (error codes) with timestamps
"""
import re
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

_COL_REF = re.compile(r'(?:\$\{)?([A-Z][A-Z0-9]+\.[A-Za-z0-9_]+)(?:\})?')
_MSG_PREFIX = re.compile(r'^([A-Z][A-Z0-9]+)\.')

# ArduPilot flight mode names by ModeNum (Copter)
_COPTER_MODES = {
    0: 'STABILIZE', 1: 'ACRO', 2: 'ALT_HOLD', 3: 'AUTO',
    4: 'GUIDED', 5: 'LOITER', 6: 'RTL', 7: 'CIRCLE',
    9: 'LAND', 11: 'DRIFT', 13: 'SPORT', 14: 'FLIP',
    15: 'AUTOTUNE', 16: 'POSHOLD', 17: 'BRAKE', 18: 'THROW',
    19: 'AVOID_ADSB', 20: 'GUIDED_NOGPS', 21: 'SMART_RTL',
    22: 'FLOWHOLD', 23: 'FOLLOW', 24: 'ZIGZAG', 25: 'SYSTEMID',
    26: 'AUTOROTATE', 27: 'AUTO_RTL',
}


def parse_required_columns(config_path: str) -> dict[str, set[str]]:
    """
    Parse feature_registry.yaml and extract which specific columns are
    needed from which message types.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    needed: dict[str, set[str]] = {}
    for feat_name, rules in config.get('features', {}).items():
        p1 = rules.get('priority_1', '')
        if p1:
            _add_column(p1, needed)
        fallback = rules.get('fallback', '')
        if fallback:
            for match in _COL_REF.finditer(fallback):
                _add_column(match.group(1), needed)

    logger.info("Registry requires %d message types: %s",
                len(needed), sorted(needed.keys()))
    return needed


def _add_column(col_ref: str, needed: dict[str, set[str]]):
    m = _MSG_PREFIX.match(col_ref)
    if m:
        msg_type = m.group(1)
        field = col_ref[len(msg_type) + 1:]
        needed.setdefault(msg_type, set()).add(field)


class LogReader:
    """
    Reads ArduPilot DataFlash .bin logs into a unified DataFrame with
    vehicle metadata, flight mode timeline, and critical event log.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.metadata = {}       # PARM values: FRAME_CLASS, FRAME_TYPE, etc.
        self.events = []         # MSG and ERR entries with timestamps
        self.mode_changes = []   # MODE changes with timestamps

    def read_and_resample(self, target_hz: int = 10,
                          config_path: str = None,
                          generate_dummy: str = None) -> pd.DataFrame:
        if generate_dummy:
            return self._generate_dummy_data(scenario=generate_dummy)

        if config_path is None:
            raise ValueError(
                "config_path (feature_registry.yaml) is required."
            )

        needed = parse_required_columns(config_path)
        return self._parse_bin(needed, target_hz)

    def _parse_bin(self, needed: dict[str, set[str]],
                   target_hz: int) -> pd.DataFrame:
        try:
            from pymavlink import DFReader
        except ImportError:
            raise ImportError("pip install pymavlink")

        logger.info("Reading %s via DFReader...", self.filepath)
        log = DFReader.DFReader_binary(self.filepath, zero_time_base=True)

        msg_types = set(needed.keys())
        streams: dict[str, list[dict]] = {t: [] for t in msg_types}

        while True:
            msg = log.recv_msg()
            if msg is None:
                break
            mtype = msg.get_type()

            # ── PARM: vehicle configuration ──────────────────────────
            if mtype == 'PARM':
                try:
                    name = msg.Name
                    value = msg.Value
                    if name in ('FRAME_CLASS', 'FRAME_TYPE',
                                'MOT_PWM_MIN', 'MOT_PWM_MAX',
                                'BATT_CAPACITY', 'INS_LOG_BAT_OPT'):
                        self.metadata[name] = value
                except AttributeError:
                    pass
                continue

            # ── MODE: flight mode changes ────────────────────────────
            if mtype == 'MODE':
                try:
                    time_us = msg.TimeUS
                    mode_num = getattr(msg, 'ModeNum',
                                       getattr(msg, 'Mode', None))
                    mode_name = getattr(msg, 'Name', None)
                    if mode_name is None and mode_num is not None:
                        mode_name = _COPTER_MODES.get(
                            int(mode_num), f'MODE_{mode_num}'
                        )
                    self.mode_changes.append({
                        'TimeUS': time_us,
                        'mode': mode_name or f'UNKNOWN',
                        'mode_num': mode_num,
                    })
                except AttributeError:
                    pass
                continue

            # ── MSG: text warnings from the flight controller ────────
            if mtype == 'MSG':
                try:
                    self.events.append({
                        'TimeUS': msg.TimeUS,
                        'type': 'MSG',
                        'text': msg.Message,
                    })
                except AttributeError:
                    pass
                continue

            # ── ERR: error codes ─────────────────────────────────────
            if mtype == 'ERR':
                try:
                    self.events.append({
                        'TimeUS': msg.TimeUS,
                        'type': 'ERR',
                        'text': f"Subsys={msg.Subsys} Code={msg.ECode}",
                        'subsys': msg.Subsys,
                        'ecode': msg.ECode,
                    })
                except AttributeError:
                    pass
                continue

            # ── Telemetry: extract only needed fields ────────────────
            if mtype not in msg_types:
                continue

            row = {'TimeUS': msg.TimeUS}
            for field in needed[mtype]:
                try:
                    row[f"{mtype}.{field}"] = getattr(msg, field)
                except AttributeError:
                    pass
            streams[mtype].append(row)

        # Build DataFrames
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

        # ── Forward-fill flight mode across the time-series ──────────
        if self.mode_changes:
            mode_df = pd.DataFrame(self.mode_changes)
            mode_df['TimeUS'] = pd.to_timedelta(mode_df['TimeUS'], unit='us')
            mode_df = mode_df.set_index('TimeUS')
            mode_df = mode_df[['mode']].rename(columns={'mode': '__flight_mode__'})
            merged = merged.join(mode_df, how='left')
            merged['__flight_mode__'] = merged['__flight_mode__'].ffill().fillna('UNKNOWN')
        else:
            merged['__flight_mode__'] = 'UNKNOWN'

        # Convert event timestamps to timedeltas
        for evt in self.events:
            evt['time_td'] = pd.to_timedelta(evt['TimeUS'], unit='us')

        logger.info(
            "Parsed %d rows, %d columns at %d Hz. "
            "Metadata: %s. Mode changes: %d. Events: %d.",
            len(merged), len(merged.columns), target_hz,
            self.metadata, len(self.mode_changes), len(self.events),
        )
        return merged

    # ------------------------------------------------------------------ #
    #  Dummy data generators                                               #
    # ------------------------------------------------------------------ #
    def _generate_dummy_data(self, scenario: str = 'motor_loss') -> pd.DataFrame:
        generators = {
            'motor_loss': self._dummy_motor_loss,
            'gps_glitch': self._dummy_gps_glitch,
            'vibration': self._dummy_vibration,
        }
        gen = generators.get(scenario)
        if gen is None:
            raise ValueError(f"Unknown scenario '{scenario}'.")
        logger.info("Generating dummy '%s' scenario...", scenario)

        # Populate dummy metadata
        self.metadata = {'FRAME_CLASS': 1, 'FRAME_TYPE': 1}
        self.mode_changes = [{'TimeUS': 0, 'mode': 'GUIDED', 'mode_num': 4}]
        self.events = [
            {'TimeUS': 10_000_000, 'type': 'MSG',
             'text': 'SIM_ENGINE_FAIL=1', 'time_td': pd.Timedelta('10s')},
        ]
        return gen()

    def _dummy_motor_loss(self) -> pd.DataFrame:
        n = 200
        t = pd.timedelta_range(start='0s', periods=n, freq='100ms')
        df = pd.DataFrame(index=t)
        fault_start = 100

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
        df['GPS.HDop'] = np.random.normal(0.8, 0.1, n).clip(0.5)
        df['GPS.NSats'] = 14.0
        df['NKF4.SP'] = np.random.normal(0.3, 0.05, n).clip(0.1)

        # Flight mode column
        df['__flight_mode__'] = 'GUIDED'
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
        df['__flight_mode__'] = 'AUTO'
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
        df['__flight_mode__'] = 'LOITER'
        return df
