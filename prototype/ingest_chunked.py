#!/usr/bin/env python3
"""
ingest_chunked.py

Parses ArduPilot DataFlash .bin telemetry logs and converts specific message types
into highly compressed Parquet files. Uses chunked processing to ensure zero OOM
errors on large files.
"""

import os
import sys
import argparse
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from pymavlink import DFReader

# The specific message types to extract to save parsing time
TARGET_MESSAGE_TYPES = {'ATT', 'GPS', 'RCOU', 'BATT'}
# Default chunk size for memory buffer before writing to disk
DEFAULT_CHUNK_SIZE = 50000

def get_memory_usage_mb():
    """Returns the current memory usage of the process in MB."""
    process = psutil.Process()
    # rss: Resident Set Size - non-swapped physical memory
    return process.memory_info().rss / (1024 * 1024)

def ingest_log(filepath, output_dir, chunk_size=DEFAULT_CHUNK_SIZE):
    if not os.path.isfile(filepath):
        print(f"Error: Log file '{filepath}' does not exist.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize our memory buffers and writers
    buffers = {msg_type: [] for msg_type in TARGET_MESSAGE_TYPES}
    writers = {}
    rows_processed = {msg_type: 0 for msg_type in TARGET_MESSAGE_TYPES}

    print(f"Starting ingestion of: {filepath}")
    print(f"Targeting message types: {', '.join(TARGET_MESSAGE_TYPES)}")
    print(f"Initial Memory Usage: {get_memory_usage_mb():.2f} MB")

    try:
        # Open the log file for sequential reading
        log = DFReader.DFReader_binary(filepath)
    except Exception as e:
        print(f"Failed to open/initialize DFReader: {e}")
        sys.exit(1)

    # Helper function to flush a buffer to the ParquetWriter
    def flush_buffer(msg_type):
        records = buffers[msg_type]
        if not records:
            return
            
        # Convert dictionary list to a PyArrow Table
        table = pa.Table.from_pylist(records)

        # Ensure we have a ParquetWriter open for this message type
        if msg_type not in writers:
            out_path = os.path.join(output_dir, f"{msg_type}.parquet")
            
            # Note: ParquetWriter handles appending row groups incrementally within 
            # this execution without overwriting previously written chunks. 
            writers[msg_type] = pq.ParquetWriter(out_path, table.schema, compression='snappy')
            print(f"[{msg_type}] Created new Parquet file writer at {out_path}")

        # Write this chunk to the open ParquetWriter
        writers[msg_type].write_table(table)
        
        chunk_len = len(records)
        rows_processed[msg_type] += chunk_len
        buffers[msg_type].clear()
        
        print(f"[{msg_type}] Wrote chunk of {chunk_len} rows. "
              f"Total {msg_type}: {rows_processed[msg_type]}. "
              f"Mem: {get_memory_usage_mb():.2f} MB")

    # Main processing loop
    try:
        while True:
            msg = log.recv_msg()
            if msg is None:
                break  # End of file

            msg_type = msg.get_type()
            
            # Filter and store only target message types
            if msg_type in TARGET_MESSAGE_TYPES:
                # to_dict() returns a standard dictionary of message fields
                buffers[msg_type].append(msg.to_dict())
                
                # Check if buffer reached our memory limit
                if len(buffers[msg_type]) >= chunk_size:
                    flush_buffer(msg_type)

    except Exception as e:
        print(f"Warning: Interrupted while reading log: {e}")

    # Flush all remaining buffers at the end of the file
    print("\nLog reading complete. Flushing remaining buffers...")
    for msg_type in TARGET_MESSAGE_TYPES:
        flush_buffer(msg_type)

    # Close all ParquetWriter instances cleanly to write metadata footers
    for msg_type, writer in writers.items():
        writer.close()
        
    print("\nIngestion Final Summary:")
    for msg_type in TARGET_MESSAGE_TYPES:
        print(f" - {msg_type}: {rows_processed[msg_type]} total rows")
    print(f"Final Memory Usage: {get_memory_usage_mb():.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse ArduPilot .bin logs into chunked, compressed Parquet files.")
    parser.add_argument("log_file", help="Path to the source .bin log file.")
    parser.add_argument("-o", "--output-dir", default=".", 
                        help="Directory to save the resulting .parquet files (default: current dir).")
    parser.add_argument("-c", "--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Number of rows per chunk before writing to disk (default: {DEFAULT_CHUNK_SIZE}).")
    
    args = parser.parse_args()
    
    ingest_log(args.log_file, args.output_dir, args.chunk_size)
