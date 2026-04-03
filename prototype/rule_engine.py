"""
rule_engine.py — Temporal, Mode-Aware, YAML-Driven Diagnostic Rule Engine (DuckDB Implementation)
Evaluates declarative failure heuristics on Parquet logs via DuckDB SQL Window Functions.
"""
import logging
import yaml
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

class CausalArbiter:
    def __init__(self, ruleset: dict):
        self.ruleset = {rule.get('id', k): rule for k, rule in ruleset.items()}
        self.logger = logging.getLogger(__name__)

    def analyze_sequence(self, triggered_events: list, within_seconds: float = 30.0) -> dict:
        if not triggered_events:
            return {"root_causes": [], "downstream_symptoms": []}

        # Events sorted by fault_start logic handle upstream/downstream flow
        triggered_events.sort(key=lambda x: x['fault_start'])

        root_causes = []
        suppressed_symptoms = []

        for event in triggered_events:
            rule_id = event.get('rule_id', event.get('rule_name'))
            rule = self.ruleset.get(rule_id)
            if not rule:
                root_causes.append(event)
                continue

            demoted = False
            predecessors = rule.get('causality', {}).get('demote_if_preceded_by', [])
            
            for predecessor_id in predecessors:
                for r in root_causes:
                    if r.get('rule_id') == predecessor_id or r.get('rule_name') == predecessor_id:
                        time_diff = abs((event['fault_start'] - r['fault_start']).total_seconds())
                        if time_diff <= within_seconds:
                            demoted = True
                            event['confidence'] = min(event.get('confidence', 0.2), 0.2)
                            event['root_cause_ref'] = predecessor_id
                            suppressed_symptoms.append(event)
                            self.logger.info(f"Demoting {rule_id} as a symptom of {predecessor_id} (dt={time_diff:.1f}s)")
                            break
                if demoted:
                    break
            
            if not demoted:
                event['confidence'] = max(event.get('confidence', 0.95), 0.95)
                root_causes.append(event)

        return {
            "root_causes": root_causes,
            "downstream_symptoms": suppressed_symptoms
        }

    def check_missing_streams(self, con, rule: dict) -> bool:
        required = set(rule.get('streams', {}).get('required', []))
        
        # In duckdb, check which view/tables exist.
        try:
            views_df = con.execute("SELECT view_name FROM duckdb_views()").df()
            views = views_df['view_name'].tolist()
            
            for req in required:
                if f"feat_{req}" not in views:
                    self.logger.warning(f"Rule {rule.get('id', 'unknown')} disabled. Missing required stream: {req}")
                    return False
            return True
        except Exception:
            return False


class RuleEngine:
    def __init__(self, rules_path: str, sample_hz: int = 10):
        with open(rules_path, 'r') as f:
            self.rules_config = yaml.safe_load(f)
        self.arbiter = CausalArbiter(self.rules_config.get('rules', {}))

    def evaluate(self, con: duckdb.DuckDBPyConnection, events=None) -> list:
        # Initialize meta log
        con.execute("""
        CREATE TABLE IF NOT EXISTS diagnostic_meta_log (
            TimeUS BIGINT,
            rule_id VARCHAR,
            rule_name VARCHAR,
            confidence DOUBLE
        )
        """)

        findings = []
        for rule_name, rule in self.rules_config.get('rules', {}).items():
            if not self.arbiter.check_missing_streams(con, rule):
                continue
                
            result = self._eval_single_rule(con, rule_name, rule)
            if result is not None:
                findings.append(result)

        if not findings:
            logger.info("No faults detected.")
            return []

        causal_results = self.arbiter.analyze_sequence(findings)
        
        all_findings = causal_results['root_causes'] + causal_results['downstream_symptoms']
        all_findings.sort(key=lambda f: f['confidence'], reverse=True)
        return all_findings

    def _eval_single_rule(self, con: duckdb.DuckDBPyConnection, rule_name: str, rule: dict) -> dict:
        conditions = rule.get('conditions', [])
        logic = rule.get('logic', 'AND').upper()
        
        windows = rule.get('windows', {})
        macro_s = windows.get('macro_window_sec')
        micro_s = windows.get('micro_window_sec')
        
        target_s = macro_s if macro_s else micro_s
        if not target_s:
            return None
            
        target_us = int(target_s * 1_000_000)

        # 1. Start building the combined table SQL
        # Use simple FULL OUTER JOIN for all required features
        req_features = [cond['feature'] for cond in conditions]
        
        join_clause = f"FROM feat_{req_features[0]} t0"
        time_cols = [f"t0.TimeUS"]
        
        for i, feat in enumerate(req_features[1:], 1):
            join_clause += f" FULL OUTER JOIN feat_{feat} t{i} ON t0.TimeUS = t{i}.TimeUS"
            time_cols.append(f"t{i}.TimeUS")

        # 2. Build the condition check
        cond_clauses = []
        for i, cond in enumerate(conditions):
            cond_clauses.append(f"(t{i}.value {cond['operator']} {cond['threshold']} AND t{i}.value IS NOT NULL)")
        
        condition_sql = f" {logic} ".join(cond_clauses)
        if not condition_sql:
            return None

        # 3. Create the query using Window Functions
        query = f"""
        WITH raw_joined AS (
            SELECT COALESCE({', '.join(time_cols)}) AS TimeUS,
                   {condition_sql} AS condition_met
            {join_clause}
        ),
        -- Filter out NULL TimeUS which might result from FULL joins on sparse data
        clean_joined AS (
            SELECT * FROM raw_joined WHERE TimeUS IS NOT NULL
        ),
        -- Window function to check sustained status
        sustained AS (
            SELECT TimeUS, condition_met,
                   MIN(CAST(COALESCE(condition_met, false) AS INT)) OVER (
                       ORDER BY TimeUS
                       RANGE BETWEEN {target_us} PRECEDING AND CURRENT ROW
                   ) as is_sustained
            FROM clean_joined
        )
        SELECT MIN(TimeUS) as fault_start, MAX(TimeUS) as fault_end
        FROM sustained
        WHERE is_sustained = 1 AND condition_met = true
        """

        try:
            df_result = con.execute(query).df()
        except Exception as e:
            logger.error(f"Error evaluating SQL for rule {rule_name}: {e}")
            return None

        if df_result.empty or pd.isna(df_result['fault_start'].iloc[0]):
            return None

        fault_start_us = int(df_result['fault_start'].iloc[0]) - target_us
        fault_end_us = int(df_result['fault_end'].iloc[0])

        fault_start_td = pd.to_timedelta(fault_start_us, unit='us')
        fault_end_td = pd.to_timedelta(fault_end_us, unit='us')
        fault_duration = (fault_end_td - fault_start_td).total_seconds()

        # Insert Meta Log
        base_conf = float(rule.get('confidence', 0.5))
        rule_id = rule.get('id', rule_name)
        con.execute(f"INSERT INTO diagnostic_meta_log VALUES ({fault_start_us}, '{rule_id}', '{rule.get('name', rule_name)}', {base_conf})")

        # Collect evidence logic would query max/min within the window if desired
        evidence = []
        for cond in conditions:
            evidence.append({
                'feature': cond['feature'],
                'operator': cond['operator'],
                'threshold': float(cond['threshold']),
                'peak_value': None,
                'mean_value': None
            })

        finding = {
            'status': 'FAULT_DETECTED',
            'rule_id': rule_id,
            'rule_name': rule.get('name', rule_name),
            'severity': rule.get('severity', 'WARNING'),
            'confidence': round(base_conf, 3),
            'description': rule.get('description', '').strip(),
            'evidence': evidence,
            'fault_start': fault_start_td,
            'fault_end': fault_end_td,
            'fault_duration_s': round(fault_duration, 2),
            'flight_mode': 'UNKNOWN',
            'events_in_window': [],
            'plot_signals': rule.get('plot_signals', []),
            'triggered_window': "macro" if macro_s else "micro"
        }

        return finding
