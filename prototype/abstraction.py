"""
abstraction.py — Version-Agnostic Feature Extraction Layer

This is the core innovation of the proposal. It consumes feature_registry.yaml
and applies fallback logic to compute features from whatever columns are
actually present in the log, regardless of firmware version.

Key improvement over v1: replaces unsafe eval() with a safe ${COL} expression
parser that only supports arithmetic operations on DataFrame columns.
"""
import re
import logging
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Regex to find ${COLUMN_NAME} references in fallback expressions
_COL_REF = re.compile(r'\$\{([^}]+)\}')

# Allowed numpy functions in expressions (whitelist for safety)
_SAFE_FUNCS = {
    'abs': np.abs,
    'max': np.maximum,
    'min': np.minimum,
    'sqrt': np.sqrt,
}


class FeatureExtractor:
    """Extracts version-agnostic features from a resampled telemetry DataFrame."""

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def compute_features(self, df: pd.DataFrame,
                         window_seconds: float = 5.0,
                         sample_hz: int = 10):
        """
        Apply YAML fallback logic and rolling-window aggregation.

        Returns:
            tuple: (scalar_features: dict, timeseries_features: pd.DataFrame)
                - scalar_features: one value per feature for the rule engine
                - timeseries_features: full time-series for visualization
        """
        window_size = int(window_seconds * sample_hz)
        timeseries = pd.DataFrame(index=df.index)

        for feat_name, rules in self.config['features'].items():
            series = self._resolve_feature(feat_name, rules, df)
            timeseries[feat_name] = series

        # Rolling window aggregation → scalar features
        scalar_features = {}
        windowed = timeseries.rolling(window=window_size, min_periods=1)

        for feat_name, rules in self.config['features'].items():
            agg = rules.get('aggregation', 'max')
            try:
                if agg == 'max':
                    scalar_features[feat_name] = float(windowed[feat_name].max().max())
                elif agg == 'min':
                    scalar_features[feat_name] = float(windowed[feat_name].min().min())
                elif agg == 'mean':
                    scalar_features[feat_name] = float(windowed[feat_name].mean().mean())
                elif agg == 'std':
                    scalar_features[feat_name] = float(windowed[feat_name].std().max())
                elif agg == 'last':
                    scalar_features[feat_name] = float(timeseries[feat_name].iloc[-1])
                else:
                    scalar_features[feat_name] = float(windowed[feat_name].max().max())
            except (TypeError, ValueError):
                scalar_features[feat_name] = np.nan

        logger.info("Extracted %d features: %s",
                     len(scalar_features),
                     {k: round(v, 2) if not np.isnan(v) else 'NaN'
                      for k, v in scalar_features.items()})

        return scalar_features, timeseries

    def _resolve_feature(self, name: str, rules: dict,
                         df: pd.DataFrame) -> pd.Series:
        """
        Resolve a single feature using the priority/fallback chain.
        """
        # Try priority_1 first
        primary = rules.get('priority_1')
        if primary and primary in df.columns:
            logger.debug("  %s → resolved via priority_1 (%s)", name, primary)
            return df[primary].copy()

        # Try fallback expression
        fallback = rules.get('fallback')
        if fallback:
            if primary:
                logger.debug("  %s → priority_1 '%s' missing, using fallback",
                             name, primary)
            else:
                logger.debug("  %s → computing from fallback expression", name)
            return self._eval_safe_expr(fallback, df, name)

        # Nothing available
        logger.warning("  %s → no data source available, filling NaN", name)
        return pd.Series(np.nan, index=df.index)

    def _eval_safe_expr(self, expr: str, df: pd.DataFrame,
                        feat_name: str) -> pd.Series:
        """
        Safely evaluate an expression like:
          abs(${ATT.DesRoll} - ${ATT.Roll})
          max(${RCOU.C1}, ${RCOU.C2}, ${RCOU.C3}, ${RCOU.C4})

        Without using eval(). Supports:
          - Column references: ${COL.NAME}
          - Arithmetic: +, -, *, /
          - Functions: abs(), max(), min(), sqrt()
        """
        # Check all referenced columns exist
        refs = _COL_REF.findall(expr)
        missing = [r for r in refs if r not in df.columns]
        if missing:
            logger.warning(
                "  %s → fallback references missing columns: %s",
                feat_name, missing,
            )
            return pd.Series(np.nan, index=df.index)

        # Handle function-style expressions: max(...), min(...), abs(...)
        # Check if expression is a function call like max(${A}, ${B}, ...)
        func_match = re.match(r'^(\w+)\((.+)\)$', expr.strip())
        if func_match:
            func_name = func_match.group(1)
            args_str = func_match.group(2)

            if func_name in ('max', 'min'):
                # Split args by comma, resolve each
                arg_exprs = [a.strip() for a in args_str.split(',')]
                series_list = []
                for arg in arg_exprs:
                    s = self._resolve_column_or_simple(arg, df)
                    if s is not None:
                        series_list.append(s)
                if not series_list:
                    return pd.Series(np.nan, index=df.index)
                result = series_list[0]
                reducer = np.maximum if func_name == 'max' else np.minimum
                for s in series_list[1:]:
                    result = reducer(result, s)
                return result

            elif func_name == 'abs':
                inner = self._resolve_arithmetic(args_str, df)
                return np.abs(inner) if inner is not None else pd.Series(np.nan, index=df.index)

            elif func_name == 'sqrt':
                inner = self._resolve_column_or_simple(args_str, df)
                return np.sqrt(inner) if inner is not None else pd.Series(np.nan, index=df.index)

        # Plain arithmetic expression: ${A} - ${B}, ${A} + ${B}, etc.
        return self._resolve_arithmetic(expr, df)

    def _resolve_arithmetic(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """Resolve simple binary arithmetic: ${A} op ${B}"""
        # Try to split on +, -, *, /
        for op_char, op_func in [('+', lambda a, b: a + b),
                                  ('-', lambda a, b: a - b),
                                  ('*', lambda a, b: a * b),
                                  ('/', lambda a, b: a / b)]:
            # Split on the operator, but not inside ${...}
            parts = re.split(r'(?<!\$\{[^}]*)\s*\%s\s*' % re.escape(op_char), expr)
            if len(parts) == 2:
                left = self._resolve_column_or_simple(parts[0].strip(), df)
                right = self._resolve_column_or_simple(parts[1].strip(), df)
                if left is not None and right is not None:
                    return op_func(left, right)

        # If no operator found, try as a single column reference
        return self._resolve_column_or_simple(expr, df)

    def _resolve_column_or_simple(self, token: str,
                                   df: pd.DataFrame):
        """Resolve a ${COL} reference or a numeric literal."""
        token = token.strip()
        col_match = _COL_REF.match(token)
        if col_match:
            col = col_match.group(1)
            if col in df.columns:
                return df[col]
            return None
        # Try parsing as a float constant
        try:
            return float(token)
        except ValueError:
            return None
