"""
rule_engine.py — Declarative, YAML-Driven Diagnostic Rule Engine

Loads diagnostic rules from rules.yaml and evaluates them against the
scalar features extracted by the abstraction layer. Rules are fully
declarative — the community can add new failure heuristics by editing
the YAML file without touching Python code.

Confidence is dynamically boosted based on how far above threshold
each condition's value is.
"""
import logging
import yaml
import numpy as np

logger = logging.getLogger(__name__)

# Operator dispatch table
_OPERATORS = {
    '>':  lambda val, thresh: val > thresh,
    '>=': lambda val, thresh: val >= thresh,
    '<':  lambda val, thresh: val < thresh,
    '<=': lambda val, thresh: val <= thresh,
    '==': lambda val, thresh: val == thresh,
}


class RuleEngine:
    """Evaluates declarative diagnostic rules against extracted features."""

    def __init__(self, rules_path: str):
        with open(rules_path, 'r') as f:
            self.rules_config = yaml.safe_load(f)

    def evaluate(self, features: dict) -> list[dict]:
        """
        Evaluate all rules against the feature vector.

        Args:
            features: dict of {feature_name: scalar_value} from the
                      abstraction layer.

        Returns:
            List of triggered diagnostic findings, sorted by confidence
            (highest first). Each finding is a dict with:
            - rule_name, severity, confidence, root_cause, evidence,
              suggested_fix, plot_signals
        """
        findings = []

        for rule_name, rule in self.rules_config['rules'].items():
            result = self._eval_single_rule(rule_name, rule, features)
            if result is not None:
                findings.append(result)

        # Sort by confidence descending
        findings.sort(key=lambda f: f['confidence'], reverse=True)

        if not findings:
            logger.info("No faults detected — all rules passed.")
            return [{
                'status': 'OK',
                'root_cause': 'none',
                'severity': 'INFO',
                'confidence': 1.0,
                'message': 'No anomalies detected in the log.',
            }]

        return findings

    def _eval_single_rule(self, rule_name: str, rule: dict,
                          features: dict) -> dict | None:
        """Evaluate one rule. Returns a finding dict or None."""
        conditions = rule.get('conditions', [])
        logic = rule.get('logic', 'AND').upper()

        condition_results = []
        evidence = []

        for cond in conditions:
            feat_name = cond['feature']
            op_str = cond['operator']
            threshold = float(cond['threshold'])

            value = features.get(feat_name)
            if value is None or np.isnan(value):
                condition_results.append(False)
                continue

            op_func = _OPERATORS.get(op_str)
            if op_func is None:
                logger.warning("Unknown operator '%s' in rule '%s'",
                               op_str, rule_name)
                condition_results.append(False)
                continue

            triggered = op_func(value, threshold)
            condition_results.append(triggered)

            if triggered:
                evidence.append({
                    'feature': feat_name,
                    'value': round(float(value), 2),
                    'operator': op_str,
                    'threshold': threshold,
                })

        # Combine conditions
        if logic == 'AND':
            triggered = all(condition_results) and len(condition_results) > 0
        elif logic == 'OR':
            triggered = any(condition_results)
        else:
            triggered = all(condition_results) and len(condition_results) > 0

        if not triggered:
            return None

        # Compute dynamic confidence: base + boost for exceeding thresholds
        base_conf = float(rule.get('confidence', 0.5))
        if evidence:
            # Average ratio of value/threshold across triggered conditions
            ratios = []
            for e in evidence:
                if e['threshold'] != 0:
                    ratios.append(abs(e['value'] / e['threshold']))
            if ratios:
                avg_ratio = sum(ratios) / len(ratios)
                # Boost: the further above threshold, the more confident
                boost = min(0.09, (avg_ratio - 1.0) * 0.05)
                base_conf = min(0.99, base_conf + max(0, boost))

        finding = {
            'status': 'FAULT_DETECTED',
            'rule_name': rule_name,
            'root_cause': rule_name,
            'severity': rule.get('severity', 'WARNING'),
            'confidence': round(base_conf, 3),
            'description': rule.get('description', '').strip(),
            'evidence': evidence,
            'suggested_fix': rule.get('suggested_fix', '').strip(),
            'plot_signals': rule.get('plot_signals', []),
        }

        logger.info("Rule '%s' TRIGGERED (confidence: %.2f)",
                     rule_name, base_conf)
        return finding
