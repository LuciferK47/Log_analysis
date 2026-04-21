import argparse
import os
import sys
import shutil

# Add prototype to sys path to cleanly import backend scripts
sys.path.append(os.path.join(os.path.dirname(__file__), "prototype"))

from ingest_chunked import ingest_log
from rule_engine_polars import evaluate_pipeline

def orchestrate(log_path, rules_path):
    print(f"\n[{'='*40}]")
    print(f" ARDUPILOT DIAGNOSTIC CLI ORCHESTRATOR ")
    print(f"[{'='*40}]")
    print(f"Target Log  : {log_path}")
    print(f"Target Rules: {rules_path}")

    # Step 1: Ingest
    basename = os.path.basename(log_path)
    tmp_dir = os.path.join(os.path.dirname(log_path), f".tmp_{basename}_parquet")
    print(f"\n>>> [Step 1/3] Converting Binary Log to Parquet format...")
    
    ingest_log(log_path, tmp_dir)
    
    att_path = os.path.join(tmp_dir, "ATT.parquet")
    rcou_path = os.path.join(tmp_dir, "RCOU.parquet")

    # Step 2: Evaluate
    print("\n>>> [Step 2/3] Passing telemetry to Diagnostic Engine...")
    
    evaluate_pipeline(att_path, rcou_path, rules_path)

    # Step 3: Cleanup
    print(f"\n>>> [Step 3/3] Orchestrator Cleanup")
    print(f"Sweeping temporary ingested data: {tmp_dir}...")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print("Cleanup successful. Execution complete.\n")

def main():
    parser = argparse.ArgumentParser(description="ArduPilot Log Diagnostics CLI (Polars + XGBoost Hybrid Engine)")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a flight log against rules logic and ML fallback")
    analyze_parser.add_argument("--log", required=True, help="Path to the flight .bin log file")
    analyze_parser.add_argument("--rules", default="prototype/rules.yaml", help="Path to the rules.yaml file")
    
    args = parser.parse_args()
    
    if args.command == "analyze":
        orchestrate(args.log, args.rules)

if __name__ == "__main__":
    main()
