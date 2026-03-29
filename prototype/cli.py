"""
cli.py — ArduPilot Log Diagnostic Tool (CLI)

Entry point:
  Log Ingestion → Feature Abstraction → Temporal Rule Engine → Plotly Report

Outputs:
  - Colored terminal diagnostics with fault windows, flight modes, and events
  - Interactive HTML report (Plotly) for pan/zoom/hover analysis
  - JSON machine-readable report
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

# ── ANSI colors ──────────────────────────────────────────────────────────
RED = '\033[91m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
CYAN = '\033[96m'
BOLD = '\033[1m'
DIM = '\033[2m'
RESET = '\033[0m'

SEVERITY_COLORS = {
    'CRITICAL': RED,
    'WARNING': YELLOW,
    'INFO': GREEN,
}


def print_banner():
    print(f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════════╗
║   ArduPilot Log Diagnostic Tool — GSoC 2026 Proto    ║
║   AI-Assisted Log Diagnosis & Root-Cause Detection   ║
╚══════════════════════════════════════════════════════╝{RESET}
""")


def print_metadata(reader: 'LogReader'):
    """Print extracted vehicle metadata."""
    if reader.metadata:
        print(f"  {BOLD}Vehicle Config:{RESET}")
        for k, v in reader.metadata.items():
            print(f"    {k}: {v}")
    if reader.mode_changes:
        modes = [m['mode'] for m in reader.mode_changes]
        print(f"  {BOLD}Flight Modes:{RESET} {' → '.join(modes)}")
    if reader.events:
        print(f"  {BOLD}Log Events:{RESET} {len(reader.events)} "
              f"MSG/ERR messages recorded")
    print()


def print_finding(finding: dict, index: int):
    status = finding.get('status', 'OK')
    if status == 'OK':
        print(f"  {GREEN}{BOLD}✓ No anomalies detected.{RESET}")
        return

    severity = finding.get('severity', 'INFO')
    color = SEVERITY_COLORS.get(severity, RESET)
    rule = finding.get('rule_name', 'unknown').replace('_', ' ').title()
    conf = finding.get('confidence', 0)
    duration = finding.get('fault_duration_s', 0)
    mode = finding.get('flight_mode', 'UNKNOWN')

    print(f"  {color}{BOLD}[{severity}] Finding #{index}: {rule}{RESET}")
    print(f"    Confidence  : {conf:.0%}")

    fs = finding.get('fault_start')
    fe = finding.get('fault_end')
    if fs is not None and fe is not None:
        print(f"    Fault Window: {fs.total_seconds():.1f}s → "
              f"{fe.total_seconds():.1f}s  "
              f"(duration: {duration:.1f}s)")
    print(f"    Flight Mode : {mode}")
    print(f"    Description : {finding.get('description', 'N/A')}")

    evidence = finding.get('evidence', [])
    if evidence:
        print(f"    Evidence:")
        for e in evidence:
            peak = e.get('peak_value', '?')
            print(f"      • {e['feature']}: peak={peak} "
                  f"({e['operator']} {e['threshold']})")

    # Print correlated events
    fault_events = finding.get('events_in_window', [])
    if fault_events:
        print(f"    {DIM}Correlated Events:{RESET}")
        for evt in fault_events[:5]:
            print(f"      {DIM}[{evt['time_s']:.1f}s] "
                  f"{evt['type']}: {evt['text']}{RESET}")

    fix = finding.get('suggested_fix', '')
    if fix:
        print(f"    {YELLOW}Suggested Fix: {fix}{RESET}")
    print()


def _serialize_finding(finding: dict) -> dict:
    out = {}
    for k, v in finding.items():
        if isinstance(v, pd.Timedelta):
            out[k] = v.total_seconds()
        elif isinstance(v, list):
            out[k] = v
        else:
            out[k] = v
    return out


def main():
    parser = argparse.ArgumentParser(
        description='ArduPilot Log Diagnostic Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 cli.py --log flight.BIN --plot -v
  python3 cli.py --dummy motor_loss --plot
  python3 cli.py --log flight.BIN --plot --output report.json
        """,
    )

    parser.add_argument('--log', type=str, default='dummy.bin',
                        help='Path to .bin DataFlash log file')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to feature_registry.yaml')
    parser.add_argument('--rules', type=str, default=None,
                        help='Path to rules.yaml')
    parser.add_argument('--dummy', type=str, default=None,
                        choices=['motor_loss', 'gps_glitch', 'vibration'],
                        help='Simulated fault scenario')
    parser.add_argument('--plot', action='store_true',
                        help='Generate interactive HTML diagnostic report')
    parser.add_argument('--plot-output', type=str,
                        default='diagnostic_report.html',
                        help='Output path for the HTML report')
    parser.add_argument('--output', type=str, default=None,
                        help='Save JSON report to this path')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    print_banner()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config or os.path.join(
        script_dir, 'feature_registry.yaml'
    )
    rules_path = args.rules or os.path.join(script_dir, 'rules.yaml')

    if not os.path.exists(config_path):
        print(f"{RED}Error: Feature registry not found at "
              f"{config_path}{RESET}")
        sys.exit(1)
    if not os.path.exists(rules_path):
        print(f"{RED}Error: Rules file not found at "
              f"{rules_path}{RESET}")
        sys.exit(1)

    t_start = time.time()

    # ── Stage 1: Ingestion ───────────────────────────────────────────────
    print(f"{CYAN}[1/4] Log Ingestion...{RESET}")
    reader = LogReader(args.log)
    df = reader.read_and_resample(
        target_hz=10,
        config_path=config_path,
        generate_dummy=args.dummy,
    )

    if df.empty:
        print(f"{RED}Error: No data extracted from the log.{RESET}")
        sys.exit(1)

    print(f"  → {len(df)} rows, {len(df.columns)} columns extracted.")
    print_metadata(reader)

    # ── Stage 2: Feature Abstraction ─────────────────────────────────────
    print(f"{CYAN}[2/4] Feature Abstraction...{RESET}")
    extractor = FeatureExtractor(config_path)
    features_ts = extractor.compute_features(df)

    # Carry the flight mode column through
    if '__flight_mode__' in df.columns:
        features_ts['__flight_mode__'] = df['__flight_mode__']

    print(f"  → {len(features_ts.columns) - 1} feature time-series "
          f"computed.")

    # ── Stage 3: Temporal Rule Evaluation ────────────────────────────────
    print(f"{CYAN}[3/4] Temporal Rule Evaluation...{RESET}")
    engine = RuleEngine(rules_path, sample_hz=10)
    findings = engine.evaluate(features_ts, events=reader.events)

    # ── Stage 4: Output ──────────────────────────────────────────────────
    elapsed = time.time() - t_start
    print(f"{CYAN}[4/4] Results ({elapsed:.2f}s)...{RESET}")
    print()

    for i, finding in enumerate(findings, 1):
        print_finding(finding, i)

    # ── Optional: Interactive HTML report ────────────────────────────────
    if args.plot:
        print(f"{CYAN}Generating interactive report → "
              f"{args.plot_output}{RESET}")
        generate_diagnostic_plot(
            df, findings,
            events=reader.events,
            output_path=args.plot_output,
        )
        print(f"  → Open {args.plot_output} in a browser to "
              f"pan, zoom, and inspect the telemetry.")

    # ── Optional: JSON report ────────────────────────────────────────────
    serializable_findings = [_serialize_finding(f) for f in findings]

    report = {
        'log_file': args.log,
        'scenario': args.dummy or 'real_log',
        'vehicle_metadata': reader.metadata,
        'mode_changes': reader.mode_changes,
        'rows_analyzed': len(df),
        'features_computed': [
            c for c in features_ts.columns if not c.startswith('__')
        ],
        'findings': serializable_findings,
        'elapsed_seconds': round(elapsed, 3),
    }

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n  {GREEN}JSON report saved to {args.output}{RESET}")
    else:
        print(f"\n{BOLD}--- JSON Report ---{RESET}")
        print(json.dumps(report, indent=2, default=str))


if __name__ == '__main__':
    main()
