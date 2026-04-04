"""
cli.py — ArduPilot Log Diagnostic Tool (CLI) [DuckDB Iteration]
"""
import argparse
import json
import logging
import os
import sys
import shutil

import pandas as pd

from ingestion import LogReader
from abstraction import FeatureExtractor
from rule_engine import RuleEngine
from visualize import generate_diagnostic_plot
try:
    from rag_pipeline import ArduPilotRAG
except ImportError:
    ArduPilotRAG = None

CYAN = '\033[96m'
GREEN = '\033[92m'
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

    print(f"{CYAN}[1/4] Log Ingestion (DuckDB/Parquet)...{RESET}")
    reader = LogReader(args.log)
    
    try:
        con = reader.read_and_resample(target_hz=10, config_path=config_path, generate_dummy=args.dummy)

        print(f"{CYAN}[2/4] Feature Abstraction (Dynamic SQL)...{RESET}")
        extractor = FeatureExtractor(config_path)
        extractor.compute_features(con)

        print(f"{CYAN}[3/4] Temporal Rule Evaluation & Causal Arbiter...{RESET}")
        engine = RuleEngine(rules_path, sample_hz=10)
        findings = engine.evaluate(con, events=reader.events)

        if findings and ArduPilotRAG is not None:
            print(f"{CYAN}[+] Querying RAG Pipeline for dynamic fixes...{RESET}")
            try:
                rag = ArduPilotRAG()
                for finding in findings:
                    if finding.get('confidence', 0) > 0.8:
                        finding['suggested_fix'] = rag.generate_fix_suggestion(finding, None)
            except Exception as e:
                logging.warning(f"RAG failed: {e}")

        print(f"{CYAN}[4/4] Generating Outputs...{RESET}")
        
        # Optional: fetch back raw data to Pandas for visualize.py
        # because visualize.py hasn't been adapted to query DuckDB directly yet
        # We will just pull required tables back for plotting if they exist to keep visualize happy.
        
        # Getting a combined dataframe to pass to visualize.py
        # Or ideally modifying visualize.py to accept DuckDB. For now we will create a mock df
        # with the signals required to not break visualize.py without further edits.
        
        plot_signals = set()
        for f in findings:
            plot_signals.update(f.get('plot_signals', []))
            
        dfs = []
        
        for sig in plot_signals:
            table_col = sig.split('.')
            if len(table_col) == 2:
                tbl, col = table_col
                try:
                    sig_df = con.execute(f"SELECT TimeUS, {col} as '{sig}' FROM {tbl} WHERE TimeUS IS NOT NULL").df()
                    sig_df['TimeUS'] = pd.to_timedelta(sig_df['TimeUS'], unit='us')
                    sig_df.set_index('TimeUS', inplace=True)
                    dfs.append(sig_df)
                except Exception:
                    pass
                    
        try:
            mode_df = con.execute("SELECT TimeUS, mode as '__flight_mode__' FROM mode_changes").df()
            mode_df['TimeUS'] = pd.to_timedelta(mode_df['TimeUS'], unit='us')
            mode_df.set_index('TimeUS', inplace=True)
            dfs.append(mode_df)
        except Exception:
            pass
            
        if dfs:
            df_plot = dfs[0].join(dfs[1:], how='outer').ffill()
        else:
            df_plot = pd.DataFrame()

        try:
            plot_path = generate_diagnostic_plot(df_plot, findings, events=reader.events, output_path=args.plot_output)
        except Exception as e:
            logging.warning(f"Plot generation failed: {e}")
            plot_path = None

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
            
        print(f"{GREEN}Done! JSON report to {args.output}{RESET}")
        
    finally:
        # GUARANTEE temp directory cleanup to avoid dead-swap buildup
        # specifically using the PID-versioned temp_dir to be concurrency safe!
        if hasattr(reader, 'duckdb_tmp'):
            shutil.rmtree(reader.duckdb_tmp, ignore_errors=True)
        if hasattr(reader, 'temp_dir'):
            shutil.rmtree(reader.temp_dir, ignore_errors=True)

if __name__ == '__main__':
    main()
