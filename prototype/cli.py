"""
cli.py — ArduPilot Log Diagnostic Tool (CLI)

Entry point that ties the full pipeline together:
  Log Ingestion → Feature Abstraction → Rule Engine → Visualization

Usage:
  # Analyze a real SITL crash log
  python cli.py --log path/to/flight.bin

  # Quick demo with simulated motor failure
  python cli.py --dummy motor_loss

  # Analyze with custom rules and generate a plot
  python cli.py --log flight.bin --rules my_rules.yaml --plot
"""
import argparse
import json
import logging
import os
import sys
import time

from ingestion import LogReader
from abstraction import FeatureExtractor
from rule_engine import RuleEngine
from visualize import generate_diagnostic_plot

# ── ANSI colors for terminal output ──────────────────────────────────────
RED = '\033[91m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
CYAN = '\033[96m'
BOLD = '\033[1m'
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


def print_finding(finding: dict, index: int):
    """Pretty-print a single diagnostic finding."""
    status = finding.get('status', 'OK')
    if status == 'OK':
        print(f"  {GREEN}{BOLD}✓ No anomalies detected.{RESET}")
        return

    severity = finding.get('severity', 'INFO')
    color = SEVERITY_COLORS.get(severity, RESET)
    rule = finding.get('rule_name', 'unknown').replace('_', ' ').title()
    conf = finding.get('confidence', 0)

    print(f"  {color}{BOLD}[{severity}] Finding #{index}: {rule}{RESET}")
    print(f"    Confidence : {conf:.0%}")
    print(f"    Description: {finding.get('description', 'N/A')}")

    evidence = finding.get('evidence', [])
    if evidence:
        print(f"    Evidence:")
        for e in evidence:
            print(f"      • {e['feature']}: {e['value']} "
                  f"({e['operator']} {e['threshold']})")

    fix = finding.get('suggested_fix', '')
    if fix:
        print(f"    {YELLOW}Suggested Fix: {fix}{RESET}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='ArduPilot Log Diagnostic Tool — '
                    'AI-Assisted Root-Cause Detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py --log flight.bin
  python cli.py --dummy motor_loss
  python cli.py --dummy gps_glitch --plot --output report.json
  python cli.py --log flight.bin --rules custom_rules.yaml --plot
        """,
    )

    parser.add_argument('--log', type=str, default='dummy.bin',
                        help='Path to .bin DataFlash log file')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to feature_registry.yaml '
                             '(default: auto-detect)')
    parser.add_argument('--rules', type=str, default=None,
                        help='Path to rules.yaml (default: auto-detect)')
    parser.add_argument('--dummy', type=str, default=None,
                        choices=['motor_loss', 'gps_glitch', 'vibration'],
                        help='Use a simulated fault scenario instead of '
                             'a real log')
    parser.add_argument('--plot', action='store_true',
                        help='Generate diagnostic visualization')
    parser.add_argument('--plot-output', type=str,
                        default='diagnostic_report.png',
                        help='Output path for the diagnostic plot')
    parser.add_argument('--output', type=str, default=None,
                        help='Save JSON report to this file path')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    # ── Setup logging ────────────────────────────────────────────────────
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    print_banner()

    # ── Auto-detect config files relative to this script ─────────────────
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config or os.path.join(script_dir, 'feature_registry.yaml')
    rules_path = args.rules or os.path.join(script_dir, 'rules.yaml')

    if not os.path.exists(config_path):
        print(f"{RED}Error: Feature registry not found at {config_path}{RESET}")
        sys.exit(1)
    if not os.path.exists(rules_path):
        print(f"{RED}Error: Rules file not found at {rules_path}{RESET}")
        sys.exit(1)

    t_start = time.time()

    # ── Stage 1: Ingestion ───────────────────────────────────────────────
    print(f"{CYAN}[1/4] Log Ingestion...{RESET}")
    reader = LogReader(args.log)
    df = reader.read_and_resample(
        target_hz=10,
        generate_dummy=args.dummy,
    )

    if df.empty:
        print(f"{RED}Error: No data extracted from the log.{RESET}")
        sys.exit(1)

    print(f"  → {len(df)} rows, {len(df.columns)} columns extracted.")

    # ── Stage 2: Feature Abstraction ─────────────────────────────────────
    print(f"{CYAN}[2/4] Feature Abstraction...{RESET}")
    extractor = FeatureExtractor(config_path)
    scalar_features, timeseries = extractor.compute_features(df)

    print(f"  → {len(scalar_features)} features computed.")

    # ── Stage 3: Rule Evaluation ─────────────────────────────────────────
    print(f"{CYAN}[3/4] Rule Evaluation...{RESET}")
    engine = RuleEngine(rules_path)
    findings = engine.evaluate(scalar_features)

    # ── Stage 4: Output ──────────────────────────────────────────────────
    elapsed = time.time() - t_start
    print(f"{CYAN}[4/4] Results ({elapsed:.2f}s)...{RESET}")
    print()

    for i, finding in enumerate(findings, 1):
        print_finding(finding, i)

    # ── Optional: Generate diagnostic plot ───────────────────────────────
    if args.plot:
        print(f"{CYAN}Generating diagnostic plot → {args.plot_output}{RESET}")
        generate_diagnostic_plot(df, findings, args.plot_output)
        print(f"  → Saved to {args.plot_output}")

    # ── Optional: Save JSON report ───────────────────────────────────────
    report = {
        'log_file': args.log,
        'scenario': args.dummy or 'real_log',
        'rows_analyzed': len(df),
        'features': {k: round(v, 4) if not (v != v) else None
                     for k, v in scalar_features.items()},
        'findings': findings,
        'elapsed_seconds': round(elapsed, 3),
    }

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n  {GREEN}JSON report saved to {args.output}{RESET}")
    else:
        print(f"\n{BOLD}--- JSON Report ---{RESET}")
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
