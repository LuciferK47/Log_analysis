"""
ingestion.py — ArduPilot DataFlash Log Ingestion Layer

Reads .bin DataFlash logs using pymavlink's DFReader. Dynamically determines
which message types and fields to extract from feature_registry.yaml.

Domain-aware features:
  - Parses PARM table for vehicle configuration (FRAME_CLASS, FRAME_TYPE)
  - Extracts MODE messages
  - Collects MSG (text warnings) and ERR (error codes) with timestamps
  - Streams output directly into columnar Parquet files sharded by message type
  - Initializes a DuckDB connection and mounts shards as raw tables
"""
import logging
import os
import re
import tempfile
import yaml

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_COL_REF = re.compile(r'(?:\$\{)?([A-Z][A-Z0-9]+\.[A-Za-z0-9_]+)(?:\})?')
_MSG_PREFIX = re.compile(r'^([A-Z][A-Z0-9]+)\.')

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
    Reads ArduPilot DataFlash .bin logs into DuckDB via Parquet shards.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.metadata = {}
        self.events = []
        self.mode_changes = []
        self.temp_dir = tempfile.mkdtemp(prefix="ardupilot_log_")

    def read_and_resample(self, target_hz: int = 10,
                          config_path: str = None,
                          generate_dummy: str = None) -> duckdb.DuckDBPyConnection:
        if config_path is None:
            raise ValueError("config_path (feature_registry.yaml) is required.")

        if generate_dummy:
            # Dummy generation for DuckDB architecture is mocked out for step 1
            return duckdb.connect(':memory:')

        needed = parse_required_columns(config_path)
        return self._parse_bin(needed, target_hz)

    def _parse_bin(self, needed: dict[str, set[str]],
                   target_hz: int) -> duckdb.DuckDBPyConnection:
        try:
            from pymavlink import DFReader
        except ImportError:
            raise ImportError("pip install pymavlink")

        logger.info("Reading %s via DFReader...", self.filepath)
        log = DFReader.DFReader_binary(self.filepath, zero_time_base=True)

        msg_types = set(needed.keys())
        streams: dict[str, list[dict]] = {t: [] for t in msg_types}

        CHUNK_SIZE = 100_000
        writers = {}

        while True:
            msg = log.recv_msg()
            if msg is None:
                break
            mtype = msg.get_type()

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
                        'mode': mode_name or 'UNKNOWN',
                        'mode_num': mode_num,
                    })
                except AttributeError:
                    pass
                continue

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

            if mtype not in msg_types:
                continue

            row = {'TimeUS': msg.TimeUS}
            for field in needed[mtype]:
                try:
                    row[field] = getattr(msg, field)
                except AttributeError:
                    row[field] = None
            streams[mtype].append(row)

            if len(streams[mtype]) >= CHUNK_SIZE:
                self._flush_stream(mtype, streams[mtype], writers)
                streams[mtype] = []

        for mtype, rows in streams.items():
            if rows:
                self._flush_stream(mtype, rows, writers)

        for writer in writers.values():
            writer.close()

        logger.info(
            "Metadata: %s. Mode changes: %d. Events: %d.",
            self.metadata, len(self.mode_changes), len(self.events),
        )
        return self._init_duckdb()

    def _flush_stream(self, mtype: str, rows: list[dict], writers: dict):
        if not rows:
            return
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        if mtype not in writers:
            path = os.path.join(self.temp_dir, f"{mtype}.parquet")
            writers[mtype] = pq.ParquetWriter(path, table.schema)
        writers[mtype].write_table(table)

    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(':memory:')
        for filename in os.listdir(self.temp_dir):
            if filename.endswith(".parquet"):
                mtype = filename[:-8]
                path = os.path.join(self.temp_dir, filename)
                con.execute(f"CREATE VIEW {mtype} AS SELECT * FROM read_parquet('{path}')")
                logger.info("Mounted %s from %s", mtype, path)
        
        if self.mode_changes:
            df_modes = pd.DataFrame(self.mode_changes)
            con.execute("CREATE TABLE mode_changes AS SELECT * FROM df_modes")
        if self.events:
            df_events = pd.DataFrame(self.events)
            con.execute("CREATE TABLE log_events AS SELECT * FROM df_events")
            
        return con
