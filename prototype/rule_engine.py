"""
rule_engine.py — Temporal, YAML-Driven Diagnostic Rule Engine

Evaluates diagnostic rules against the FULL feature time-series DataFrame,
NOT global scalars. This ensures that AND conditions must be simultaneously
true for at least N consecutive seconds to trigger, preventing false
positives from events that happened minutes apart.

Each triggered finding includes exact fault_start and fault_end timestamps
for precise visualization.
"""
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Operator dispatch table
_OPERATORS = {
    '>':  lambda series, thresh: series > thresh,
    '>=': lambda series, thresh: series >= thresh,
    '<':  lambda series, thresh: series < thresh,
    '<=': lambda series, thresh: series <= thresh,
    '==': lambda series, thresh: series == thresh,
}


class RuleEngine:
    """
    Evaluates declarative diagnostic rules against feature time-series.
    Rules trigger only when all conditions are simultaneously true for
    a minimum sustained duration.
    """

    def __init__(self, rules_path: str, sample_hz: int = 10,
                 min_fault_seconds: float = 1.0):
        """
        Args:
            rules_path: Path to rules.yaml
            sample_hz: Sampling rate of the input DataFrame
            min_fault_seconds: Minimum consecutive seconds that all
                               conditions must be true to trigger a rule.
        """
        with open(rules_path, 'r') as f:
            self.rules_config = yaml.safe_load(f)
        self.sample_hz = sample_hz
        self.min_fault_samples = int(min_fault_seconds * sample_hz)

    def evaluate(self, features_ts: pd.DataFrame) -> list[dict]:
        """
        Evaluate all rules against the feature time-series DataFrame.

        Args:
            features_ts: DataFrame from abstraction layer with one column
                         per abstract feature, indexed by time.

        Returns:
            List of triggered diagnostic findings, sorted by confidence.
            Each finding includes fault_start and fault_end timestamps.
        """
        findings = []

        for rule_name, rule in self.rules_config['rules'].items():
            result = self._eval_single_rule(rule_name, rule, features_ts)
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
                          features_ts: pd.DataFrame) -> dict | None:
        """
        Evaluate one rule against the time-series. A rule triggers only
        if its combined boolean mask has consecutive True values lasting
        at least min_fault_samples.
        """
        conditions = rule.get('conditions', [])
        logic = rule.get('logic', 'AND').upper()

        # Build a boolean mask per condition (each is a full Series)
        condition_masks = []
        evidence = []

        for cond in conditions:
            feat_name = cond['feature']
            op_str = cond['operator']
            threshold = float(cond['threshold'])

            if feat_name not in features_ts.columns:
                logger.debug("  %s: feature '%s' not in DataFrame",
                             rule_name, feat_name)
                # Missing feature → condition is False everywhere
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
                continue

            series = features_ts[feat_name]

            # Skip if all NaN
            if series.isna().all():
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
                continue

            op_func = _OPERATORS.get(op_str)
            if op_func is None:
                logger.warning("Unknown operator '%s' in rule '%s'",
                               op_str, rule_name)
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
                continue

            mask = op_func(series, threshold)
            # NaN comparisons → False
            mask = mask.fillna(False)
            condition_masks.append(mask)

            # Collect evidence from the peak value in the fault window
            evidence.append({
                'feature': feat_name,
                'operator': op_str,
                'threshold': threshold,
            })

        if not condition_masks:
            return None

        # Combine condition masks with AND or OR logic
        if logic == 'AND':
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined & m
        else:  # OR
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined | m

        # Find consecutive True runs of at least min_fault_samples
        fault_start, fault_end = self._find_sustained_fault(combined)

        if fault_start is None:
            return None

        # Enrich evidence with actual peak values during the fault window
        for e in evidence:
            feat = e['feature']
            if feat in features_ts.columns:
                fault_slice = features_ts.loc[fault_start:fault_end, feat]
                if not fault_slice.empty:
                    e['peak_value'] = round(float(fault_slice.abs().max()), 2)
                    e['mean_value'] = round(float(fault_slice.mean()), 2)

        # Dynamic confidence: base + boost from how far values exceed
        # thresholds within the fault window
        base_conf = float(rule.get('confidence', 0.5))
        ratios = []
        for e in evidence:
            if e.get('peak_value') is not None and e['threshold'] != 0:
                ratios.append(abs(e['peak_value'] / e['threshold']))
        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            boost = min(0.09, (avg_ratio - 1.0) * 0.03)
            base_conf = min(0.99, base_conf + max(0, boost))

        # Compute fault duration
        fault_duration = (fault_end - fault_start).total_seconds()

        finding = {
            'status': 'FAULT_DETECTED',
            'rule_name': rule_name,
            'root_cause': rule_name,
            'severity': rule.get('severity', 'WARNING'),
            'confidence': round(base_conf, 3),
            'description': rule.get('description', '').strip(),
            'evidence': evidence,
            'fault_start': fault_start,
            'fault_end': fault_end,
            'fault_duration_s': round(fault_duration, 2),
            'suggested_fix': rule.get('suggested_fix', '').strip(),
            'plot_signals': rule.get('plot_signals', []),
        }

        logger.info(
            "Rule '%s' TRIGGERED (confidence: %.2f, "
            "window: %.1fs–%.1fs, duration: %.1fs)",
            rule_name, base_conf,
            fault_start.total_seconds(), fault_end.total_seconds(),
            fault_duration,
        )
        return finding

    def _find_sustained_fault(self, mask: pd.Series):
        """
        Find the longest consecutive run of True values in the boolean
        mask that lasts at least min_fault_samples. Returns the start
        and end timestamps of that run, or (None, None).
        """
        if not mask.any():
            return None, None

        # Label consecutive groups
        # When mask changes from False→True or True→False, the cumsum
        # of ~mask increments, creating group IDs for True-runs.
        groups = (~mask).cumsum()

        # Only keep True groups
        true_groups = groups[mask]
        if true_groups.empty:
            return None, None

        # Count length of each True-run
        group_sizes = true_groups.groupby(true_groups).size()

        # Filter to runs >= min_fault_samples
        valid_groups = group_sizes[group_sizes >= self.min_fault_samples]

        if valid_groups.empty:
            return None, None

        # Pick the longest sustained fault
        longest_group_id = valid_groups.idxmax()
        fault_indices = true_groups[true_groups == longest_group_id].index

        return fault_indices[0], fault_indices[-1]
