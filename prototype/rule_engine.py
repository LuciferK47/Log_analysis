"""
rule_engine.py — Temporal, Mode-Aware, YAML-Driven Diagnostic Rule Engine
"""
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

_OPERATORS = {
    '>':  lambda series, thresh: series > thresh,
    '>=': lambda series, thresh: series >= thresh,
    '<':  lambda series, thresh: series < thresh,
    '<=': lambda series, thresh: series <= thresh,
    '==': lambda series, thresh: series == thresh,
}

class CausalArbiter:
    def __init__(self, ruleset: list):
        self.ruleset = {rule.get('id', k): rule for k, rule in ruleset.items()}
        self.logger = logging.getLogger(__name__)

    def analyze_sequence(self, triggered_events: list, within_seconds: float = 30.0) -> dict:
        if not triggered_events:
            return {"root_causes": [], "downstream_symptoms": []}

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

    def check_missing_streams(self, available_streams: set, rule: dict) -> bool:
        required = set(rule.get('streams', {}).get('required', []))
        if not required.issubset(available_streams):
            missing = required - available_streams
            self.logger.warning(f"Rule {rule.get('id', 'unknown')} disabled. Missing required streams: {missing}")
            return False
        return True


class RuleEngine:
    def __init__(self, rules_path: str, sample_hz: int = 10):
        with open(rules_path, 'r') as f:
            self.rules_config = yaml.safe_load(f)
        self.sample_hz = sample_hz
        self.arbiter = CausalArbiter(self.rules_config.get('rules', {}))

    def evaluate(self, features_ts: pd.DataFrame, events: list = None) -> list:
        findings = []
        events = events or []
        available_streams = set(features_ts.columns)

        for rule_name, rule in self.rules_config.get('rules', {}).items():
            if not self.arbiter.check_missing_streams(available_streams, rule):
                continue
                
            result = self._eval_single_rule(rule_name, rule, features_ts, events)
            if result is not None:
                findings.append(result)

        if not findings:
            logger.info("No faults detected.")
            return []

        causal_results = self.arbiter.analyze_sequence(findings)
        
        all_findings = causal_results['root_causes'] + causal_results['downstream_symptoms']
        all_findings.sort(key=lambda f: f['confidence'], reverse=True)
        return all_findings

    def _eval_single_rule(self, rule_name: str, rule: dict,
                          features_ts: pd.DataFrame,
                          events: list) -> dict:
        conditions = rule.get('conditions', [])
        logic = rule.get('logic', 'AND').upper()
        
        windows = rule.get('windows', {})
        macro_s = windows.get('macro_window_sec')
        micro_s = windows.get('micro_window_sec')
        
        macro_samples = max(1, int(macro_s * self.sample_hz)) if macro_s else None
        micro_samples = max(1, int(micro_s * self.sample_hz)) if micro_s else None
        ignored_modes = rule.get('ignored_modes', [])

        condition_masks = []
        evidence = []

        for cond in conditions:
            feat_name = cond['feature']
            op_str = cond['operator']
            threshold = float(cond['threshold'])

            if feat_name not in features_ts.columns:
                condition_masks.append(pd.Series(False, index=features_ts.index))
                continue

            series = features_ts[feat_name]
            if series.isna().all():
                condition_masks.append(pd.Series(False, index=features_ts.index))
                continue

            op_func = _OPERATORS.get(op_str)
            if op_func is None:
                continue

            mask = op_func(series, threshold).fillna(False)
            condition_masks.append(mask)
            evidence.append({
                'feature': feat_name,
                'operator': op_str,
                'threshold': threshold,
            })

        if not condition_masks:
            return None

        if logic == 'AND':
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined & m
        else:
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined | m

        if ignored_modes and '__flight_mode__' in features_ts.columns:
            mode_col = features_ts['__flight_mode__']
            suppressed = mode_col.isin(ignored_modes)
            combined = combined & ~suppressed

        fault_start, fault_end, triggered_window = self._find_sustained_fault_adaptive(
            combined, macro_samples, micro_samples
        )
        if fault_start is None:
            return None

        for e in evidence:
            feat = e['feature']
            if feat in features_ts.columns:
                fault_slice = features_ts.loc[fault_start:fault_end, feat]
                if not fault_slice.empty:
                    e['peak_value'] = round(float(fault_slice.abs().max()), 2)
                    e['mean_value'] = round(float(fault_slice.mean()), 2)

        base_conf = float(rule.get('confidence', 0.5))
        fault_duration = (fault_end - fault_start).total_seconds()

        fault_mode = 'UNKNOWN'
        if '__flight_mode__' in features_ts.columns:
            mode_slice = features_ts.loc[fault_start:fault_end, '__flight_mode__']
            if not mode_slice.empty:
                fault_mode = mode_slice.mode().iloc[0]

        finding = {
            'status': 'FAULT_DETECTED',
            'rule_id': rule.get('id', rule_name),
            'rule_name': rule.get('name', rule_name),
            'severity': rule.get('severity', 'WARNING'),
            'confidence': round(base_conf, 3),
            'description': rule.get('description', '').strip(),
            'evidence': evidence,
            'fault_start': fault_start,
            'fault_end': fault_end,
            'fault_duration_s': round(fault_duration, 2),
            'flight_mode': fault_mode,
            'events_in_window': [],
            'plot_signals': rule.get('plot_signals', []),
            'triggered_window': triggered_window
        }

        return finding

    def _find_sustained_fault_adaptive(self, mask: pd.Series, macro_samples: int = None, micro_samples: int = None):
        if not mask.any():
            return None, None, None
        
        groups = (~mask).cumsum()
        true_groups = groups[mask]
        if true_groups.empty:
            return None, None, None
            
        group_sizes = true_groups.groupby(true_groups).size()
        
        longest_id = group_sizes.idxmax()
        max_size = group_sizes[longest_id]
        
        triggered_window = None
        if macro_samples and max_size >= macro_samples:
            triggered_window = "macro"
        elif micro_samples and max_size >= micro_samples:
            triggered_window = "micro"
            
        if not triggered_window:
            return None, None, None
            
        fault_indices = true_groups[true_groups == longest_id].index
        return fault_indices[0], fault_indices[-1], triggered_window
