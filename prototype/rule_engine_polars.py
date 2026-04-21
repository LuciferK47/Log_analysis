#!/usr/bin/env python3
"""
rule_engine_polars.py

Implements "Adaptive Temporal Windowing" across ArduPilot sensors 
(ATT and RCOU) using Polars strictly via lazy evaluation.
Upgraded into a YAML-driven Rule Evaluator and Causal Arbiter.
"""

import time
import operator
import yaml
import polars as pl
import json
import xgboost as xgb

# Operator function mappings for dynamic YAML parsing
OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne
}

def load_rules(filepath="prototype/rules.yaml"):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)["rules"]

def build_condition_expr(cond):
    """Dynamically builds a polars condition expression from a YAML dictionary."""
    col_expr = pl.col(cond["feature"])
    op_func = OPS[cond["operator"]]
    val = cond["threshold"]
    return op_func(col_expr, val)

def generate_diagnostic_report(rule_name, onset_row, confidence=1.0, description="Rule triggered via deterministic causal arbiter.", rag_context=None):
    clean_rule_name = rule_name.replace("rule_", "") if rule_name.startswith("rule_") else rule_name
    
    report = {
        "status": "failure",
        "confidence": confidence, 
        "rule_triggered": clean_rule_name,
        "description": description,
        "evidence": {
            "RCOU.C1": onset_row.get('C1'),
            "ATT.Roll": onset_row.get('Roll')
        },
        "timestamp_window": [onset_row.get('TimeUS'), None] 
    }
    
    if rag_context:
        report["rag_context"] = rag_context
    
    print("\n--- JSON DIAGNOSTIC REPORT ---")
    print(json.dumps(report, indent=2))
    return report

def evaluate_pipeline(att_path, rcou_path, rules_path):
    print("Loading XGBoost fallback model...")
    model = xgb.XGBClassifier()
    model.load_model("xgboost_fallback.json")

    # File isolation constraints handled via arguments
    RULES_PATH = rules_path
    
    print("Setting up lazy computation graph...")

    # 1. Lazy Evaluation ONLY via pl.scan_parquet()
    att_lf = pl.scan_parquet(att_path).select(["TimeUS", "Roll"])
    rcou_lf = pl.scan_parquet(rcou_path).select(["TimeUS", "C1"])

    # 2. Time Conversion
    att_lf = att_lf.with_columns([
        pl.from_epoch(pl.col("TimeUS"), time_unit="us").alias("timestamp")
    ]).sort("timestamp")
    
    rcou_lf = rcou_lf.with_columns([
        pl.from_epoch(pl.col("TimeUS"), time_unit="us").alias("timestamp")
    ]).sort("timestamp")

    # 3. Sensor Fusion (Time Alignment)
    joined_lf = att_lf.join_asof(rcou_lf, on="timestamp", strategy="backward")

    # 4. Adaptive Temporal Windowing
    micro_window_lf = joined_lf.rolling(index_column="timestamp", period="2s").agg([
        pl.col("Roll").var().alias("roll_var_2s"),
        pl.col("C1").max().alias("rcou_c1_max_2s")
    ])

    macro_window_lf = joined_lf.rolling(index_column="timestamp", period="30s").agg([
        pl.col("Roll").mean().alias("roll_mean_30s"),
        pl.col("C1").mean().alias("rcou_c1_mean_30s")
    ])

    # Join the windows back together
    final_lf = (
        joined_lf
        .join(micro_window_lf, on="timestamp")
        .join(macro_window_lf, on="timestamp")
    ).drop_nulls()

    # 5. DYNAMIC YAML PARSING & RULE EVALUATION
    # Parse the YAML directly in Python
    rules_data = load_rules(RULES_PATH)
    rule_exprs = []
    
    for rule_key, rule_def in rules_data.items():
        conditions = rule_def.get("conditions", [])
        if not conditions:
            continue
            
        # We only map rules whose required features apply to the columns we actually have
        required_features = [c["feature"] for c in conditions]
        available_cols = {"TimeUS", "Roll", "timestamp", "C1", 
                          "roll_var_2s", "rcou_c1_max_2s", 
                          "roll_mean_30s", "rcou_c1_mean_30s"}
                          
        if not all(rf in available_cols for rf in required_features):
            # Skip rules that reference sensors not scanned in this lazy graph (e.g., GPS)
            continue
            
        # Build first condition recursively using logic operator
        expr = build_condition_expr(conditions[0])
        logic_op = rule_def.get("logic", "AND")
        
        for cond in conditions[1:]:
            if logic_op == "AND":
                expr = expr.and_(build_condition_expr(cond))
            else:
                expr = expr.or_(build_condition_expr(cond))
        
        # Alias boolean column specifically avoiding hardcoding
        rule_alias_name = f"rule_{rule_key.lower()}"
        rule_exprs.append(expr.alias(rule_alias_name))

    if rule_exprs:
        # Generate boolean rule evaluation dynamically INTO the lazy frame BEFORE execution
        final_lf = final_lf.with_columns(rule_exprs)
    
    # 6. Benchmarking and .collect() trigger
    print("Executing combined graph (Windowing + Dynamic Rules)...")
    start_time = time.time()
    
    # Real computation happens strictly here taking complete advantage of Rust/Arrow optimizations
    result_df = final_lf.collect()
    
    exec_time = time.time() - start_time
    print(f"[Benchmarking] Complete Lazy Graph Execution Time: {exec_time:.4f} seconds")

    # Initialize RAG Pipeline
    from rag_pipeline import ArduPilotRAG
    rag_pipeline = ArduPilotRAG()
    print("RAG pipeline engaged.")

    # 7. CAUSAL ARBITER FILTERING
    print("\n--- CAUSAL ARBITER ---")
    
    anomaly_triggered = False
    
    for rule_key, rule_def in rules_data.items():
        target_rule = f"rule_{rule_key.lower()}"
        if target_rule in result_df.columns:
            # Filter precisely to the moments the rule flagged as True (Causal Arbiter)
            failed_rows = result_df.filter(pl.col(target_rule) == True)
            
            if len(failed_rows) > 0:
                print(f"Targeting root cause signature: {target_rule}")
                print(f"Triggered Anomaly States Detected: {len(failed_rows)}")
                
                # Print the VERY FIRST timestamp of failure onset
                first_fail = failed_rows.row(0, named=True)
                print("\n>> FAILURE ONSET REVEALED <<")
                print(f"Timestamp    : {first_fail['timestamp']}")
                print(f"Onset Roll   : {first_fail['Roll']:.5f}")
                print(f"Onset RCOU C1: {first_fail['C1']}")
                print("=============================")
                
                rag_context = None
                if rag_pipeline:
                    diagnosis_event = {
                        "name": rule_def.get("name", rule_key),
                        "description": rule_def.get("description", "")
                    }
                    rag_context = rag_pipeline.retrieve_context(diagnosis_event)
                
                report = generate_diagnostic_report(target_rule, first_fail, confidence=1.0, description="Rule triggered via deterministic causal arbiter.", rag_context=rag_context)
                anomaly_triggered = True
                return report

    if not anomaly_triggered:
            print("\n--- TRIGGERING ML FALLBACK ---")
            features = ['roll_var_2s', 'rcou_c1_max_2s', 'roll_mean_30s', 'rcou_c1_mean_30s']
            X = result_df.select(features).to_pandas()
            
            y_pred = model.predict(X)
            
            import numpy as np
            anomaly_indices = np.where(y_pred == 1)[0]
            
            if len(anomaly_indices) > 0:
                first_anomaly_idx = anomaly_indices[0]
                first_fail = result_df.row(first_anomaly_idx, named=True)
                
                print(f"Triggered ML Anomaly States Detected: {len(anomaly_indices)}")
                print("\n>> ML FAILURE ONSET REVEALED <<")
                print(f"Timestamp    : {first_fail['timestamp']}")
                print(f"Onset Roll   : {first_fail['Roll']:.5f}")
                print(f"Onset RCOU C1: {first_fail['C1']}")
                print("=============================")
                
                rag_context = None
                if rag_pipeline:
                    diagnosis_event = {
                        "name": "Motor/ESC Failure (ML Fallback)",
                        "description": f"Anomaly detected via ML feature correlation. RCOU channel spiked to {first_fail.get('C1')} PWM while ATT.Roll diverged to {first_fail.get('Roll')}."
                    }
                    rag_context = rag_pipeline.retrieve_context(diagnosis_event)
                
                return generate_diagnostic_report(
                    rule_name="xgboost_ml_fallback", 
                    onset_row=first_fail, 
                    confidence=0.85, 
                    description="Anomaly detected via ML feature correlation (Deterministic rules bypassed).",
                    rag_context=rag_context
                )
            else:
                print("Status: Stable. No anomalies detected.")
                return {"status": "ok", "message": "Stable. No anomalies detected."}


# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser()
#     # evaluate_pipeline(...)

