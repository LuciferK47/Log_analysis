"""
cli.py — ArduPilot Log Diagnostic Tool (CLI) [Upgraded]
Now featuring RAG pipeline, Causal Arbiter support and headless JSON output.
"""
import argparse
import json
import logging
import os
import sys
import time

import pandas as pd

from ingestion import LogReader
from abstraction import FeatureExtractor
from rule_engine import RuleEngine
from visualize import generate_diagnostic_plot
try:
    from rag_pipeline import ArduPilotRAG
except ImportError:
    ArduPilotRAG = None

# ── ANSI colors ──────────────────────────────────────────────────────────
RED = '\033[91m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
CYAN = '\033[96m'
BOLD = '\033[1m'
DIM = '\033[2m'
RESET = '\033[0m'

def _serialize_finding(finding: dict) -> dict:
    out = {}
    for k, v in finding.items():
        if isinstance(v, pd.Timedelta):
            out[k] = v.total_seconds()
        elif isinstance(v, pd.Timestamp):
            out[k] = v.isoformat()
        elif isinstance(v, list):
            out[k] = v
        else:
            out[k] = v
    return out

def main():
    parser = argparse.ArgumentParser(description='ArduPilot Log Diagnostic Tool')
    parser.add_argument('--log', type=str, default='dummy.bin')
    parser.add_argument('--config', type=str, default=None)
    parser.add_argument('--rules', type=str, default=None)
    parser.add_argument('--dummy', type=str, default=None)
    parser.add_argument('--plot-output', type=str, default='output/plot.png')
    parser.add_argument('--output', type=str, default='output/report.json')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config or os.path.join(script_dir, 'feature_registry.yaml')
    rules_path = args.rules or os.path.join(script_dir, 'rules.yaml')

    print(f"{CYAN}[1/4] Log Ingestion...{RESET}")
    reader = LogReader(args.log)
    df = reader.read_and_resample(target_hz=10, config_path=config_path, generate_dummy=args.dummy)

    print(f"{CYAN}[2/4] Feature Abstraction...{RESET}")
    extractor = FeatureExtractor(config_path)
    features_ts = extractor.compute_features(df)
    if '__flight_mode__' in df.columns:
        features_ts['__flight_mode__'] = df['__flight_mode__']

    print(f"{CYAN}[3/4] Temporal Rule Evaluation & Causal Arbiter...{RESET}")
    engine = RuleEngine(rules_path, sample_hz=10)
    findings = engine.evaluate(features_ts, events=reader.events)

    # ── RAG Pipeline Integration ───────────────────────────────────────────
    if findings and ArduPilotRAG is not None:
        print(f"{CYAN}[+] Querying RAG Pipeline for dynamic fixes...{RESET}")
        try:
            rag = ArduPilotRAG()
            for finding in findings:
                if finding.get('confidence', 0) > 0.8:
                    finding['suggested_fix'] = rag.generate_fix_suggestion(finding, None)
        except Exception as e:
            logging.warning(f"RAG failed: {e}")

    # ── Outputs ────────────────────────────────────────────────────────────
    print(f"{CYAN}[4/4] Generating Outputs...{RESET}")
    plot_path = generate_diagnostic_plot(df, findings, events=reader.events, output_path=args.plot_output)

    serializable_findings = [_serialize_finding(f) for f in findings]
    
    root_causes = [f for f in serializable_findings if not f.get('root_cause_ref')]
    downstream = [f for f in serializable_findings if f.get('root_cause_ref')]

    report = {
        'status': 'success' if findings else 'ok',
        'metadata': reader.metadata,
        'primary_root_cause': root_causes[0] if root_causes else None,
        'causal_chain': {
            'root_causes': root_causes,
            'downstream_symptoms': downstream
        },
        'artifacts': {
            'diagnostic_plot_path': plot_path
        }
    }

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    with open(args.output, 'w') as f:
        json.dump(report, f, indent=4)
        
    print(f"{GREEN}Done! Plot saved to {plot_path}, JSON report to {args.output}{RESET}")

if __name__ == '__main__':
    main()
