"""
rule_engine.py — Temporal, Mode-Aware, YAML-Driven Diagnostic Rule Engine

Evaluates diagnostic rules against the FULL feature time-series DataFrame.
Each rule specifies:
  - duration_seconds: hysteresis (minimum sustained True time)
  - ignored_modes: list of flight modes where the rule is suppressed

This prevents false positives from both noise spikes AND expected behaviour
in manual/acrobatic flight modes.
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

_DEFAULT_DURATION_SECONDS = 1.0


class RuleEngine:
    """
    Evaluates declarative diagnostic rules against feature time-series,
    with per-rule hysteresis and flight-mode awareness.
    """

    def __init__(self, rules_path: str, sample_hz: int = 10):
        with open(rules_path, 'r') as f:
            self.rules_config = yaml.safe_load(f)
        self.sample_hz = sample_hz

    def evaluate(self, features_ts: pd.DataFrame,
                 events: list[dict] = None) -> list[dict]:
        """
        Evaluate all rules against the feature time-series.
        Optionally cross-references MSG/ERR events within fault windows.

        Args:
            features_ts: DataFrame with feature columns + __flight_mode__.
            events: List of MSG/ERR event dicts from LogReader.

        Returns:
            List of triggered findings, sorted by confidence.
        """
        findings = []
        events = events or []

        for rule_name, rule in self.rules_config['rules'].items():
            result = self._eval_single_rule(
                rule_name, rule, features_ts, events
            )
            if result is not None:
                findings.append(result)

        findings.sort(key=lambda f: f['confidence'], reverse=True)

        if not findings:
            logger.info("No faults detected.")
            return [{
                'status': 'OK',
                'root_cause': 'none',
                'severity': 'INFO',
                'confidence': 1.0,
                'message': 'No anomalies detected in the log.',
            }]

        return findings

    def _eval_single_rule(self, rule_name: str, rule: dict,
                          features_ts: pd.DataFrame,
                          events: list[dict]) -> dict | None:
        conditions = rule.get('conditions', [])
        logic = rule.get('logic', 'AND').upper()
        duration_s = float(
            rule.get('duration_seconds', _DEFAULT_DURATION_SECONDS)
        )
        min_samples = max(1, int(duration_s * self.sample_hz))
        ignored_modes = rule.get('ignored_modes', [])

        # Build per-condition boolean masks
        condition_masks = []
        evidence = []

        for cond in conditions:
            feat_name = cond['feature']
            op_str = cond['operator']
            threshold = float(cond['threshold'])

            if feat_name not in features_ts.columns:
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
                continue

            series = features_ts[feat_name]
            if series.isna().all():
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
                continue

            op_func = _OPERATORS.get(op_str)
            if op_func is None:
                condition_masks.append(
                    pd.Series(False, index=features_ts.index)
                )
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

        # Combine masks
        if logic == 'AND':
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined & m
        else:
            combined = condition_masks[0]
            for m in condition_masks[1:]:
                combined = combined | m

        # ── Flight-mode suppression ──────────────────────────────────
        if ignored_modes and '__flight_mode__' in features_ts.columns:
            mode_col = features_ts['__flight_mode__']
            suppressed = mode_col.isin(ignored_modes)
            suppressed_count = (combined & suppressed).sum()
            if suppressed_count > 0:
                logger.debug(
                    "  %s: suppressed %d samples in modes %s",
                    rule_name, suppressed_count, ignored_modes,
                )
            combined = combined & ~suppressed

        # Find sustained fault
        fault_start, fault_end = self._find_sustained_fault(
            combined, min_samples
        )
        if fault_start is None:
            return None

        # Enrich evidence with peak values
        for e in evidence:
            feat = e['feature']
            if feat in features_ts.columns:
                fault_slice = features_ts.loc[fault_start:fault_end, feat]
                if not fault_slice.empty:
                    e['peak_value'] = round(float(fault_slice.abs().max()), 2)
                    e['mean_value'] = round(float(fault_slice.mean()), 2)

        # Dynamic confidence
        base_conf = float(rule.get('confidence', 0.5))
        ratios = []
        for e in evidence:
            peak = e.get('peak_value')
            if peak is not None and e['threshold'] != 0:
                ratios.append(abs(peak / e['threshold']))
        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            boost = min(0.09, (avg_ratio - 1.0) * 0.03)
            base_conf = min(0.99, base_conf + max(0, boost))

        fault_duration = (fault_end - fault_start).total_seconds()

        # Determine the dominant flight mode during the fault
        fault_mode = 'UNKNOWN'
        if '__flight_mode__' in features_ts.columns:
            mode_slice = features_ts.loc[
                fault_start:fault_end, '__flight_mode__'
            ]
            if not mode_slice.empty:
                fault_mode = mode_slice.mode().iloc[0]

        # Collect MSG/ERR events that occurred during the fault window
        fault_events = []
        fs_sec = fault_start.total_seconds()
        fe_sec = fault_end.total_seconds()
        for evt in events:
            evt_sec = evt.get('time_td', pd.Timedelta(0)).total_seconds()
            if fs_sec - 2.0 <= evt_sec <= fe_sec + 2.0:
                fault_events.append({
                    'time_s': round(evt_sec, 2),
                    'type': evt.get('type', ''),
                    'text': evt.get('text', ''),
                })

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
            'duration_threshold_s': duration_s,
            'flight_mode': fault_mode,
            'events_in_window': fault_events,
            'suggested_fix': rule.get('suggested_fix', '').strip(),
            'plot_signals': rule.get('plot_signals', []),
        }

        logger.info(
            "Rule '%s' TRIGGERED (conf=%.2f, window=%.1fs–%.1fs, "
            "duration=%.1fs, mode=%s, events=%d)",
            rule_name, base_conf,
            fault_start.total_seconds(), fault_end.total_seconds(),
            fault_duration, fault_mode, len(fault_events),
        )
        return finding

    def _find_sustained_fault(self, mask: pd.Series, min_samples: int):
        if not mask.any():
            return None, None

        groups = (~mask).cumsum()
        true_groups = groups[mask]
        if true_groups.empty:
            return None, None

        group_sizes = true_groups.groupby(true_groups).size()
        valid_groups = group_sizes[group_sizes >= min_samples]

        if valid_groups.empty:
            return None, None

        longest_id = valid_groups.idxmax()
        fault_indices = true_groups[true_groups == longest_id].index
        return fault_indices[0], fault_indices[-1]
