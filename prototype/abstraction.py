"""
abstraction.py — Version-Agnostic Feature Extraction Layer

Core innovation of the proposal. Consumes feature_registry.yaml and applies
fallback logic to compute features from whatever columns are present,
regardless of firmware version.

Key design decisions:
  - Returns the FULL time-series DataFrame (not crushed scalars) so the rule
    engine can evaluate temporal co-occurrence of conditions.
  - Uses Python's ast module for safe expression parsing instead of eval()
    or fragile regex splitting.
"""
import ast
import logging
import operator
import re
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Regex to find ${COLUMN_NAME} references in fallback expressions
_COL_REF_PATTERN = re.compile(r'\$\{([^}]+)\}')


class FeatureExtractor:
    """Extracts version-agnostic features from a resampled telemetry DataFrame."""

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply YAML fallback logic to produce a time-series DataFrame of
        abstract features. Each column in the output corresponds to one
        feature defined in the YAML registry.

        The rule engine is responsible for windowed aggregation and
        temporal evaluation — this layer only resolves raw signals into
        version-agnostic feature columns.

        Returns:
            pd.DataFrame with one column per feature, same index as input.
        """
        timeseries = pd.DataFrame(index=df.index)

        for feat_name, rules in self.config['features'].items():
            series = self._resolve_feature(feat_name, rules, df)
            timeseries[feat_name] = series

        logger.info("Extracted %d feature time-series, %d rows each.",
                     len(timeseries.columns), len(timeseries))
        return timeseries

    def _resolve_feature(self, name: str, rules: dict,
                         df: pd.DataFrame) -> pd.Series:
        """Resolve a single feature using the priority/fallback chain."""
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
                logger.debug("  %s → computing from fallback expression",
                             name)
            return self._eval_safe_expr(fallback, df, name)

        # Nothing available
        logger.warning("  %s → no data source available, filling NaN", name)
        return pd.Series(np.nan, index=df.index)

    # ------------------------------------------------------------------ #
    #  Safe AST-based expression evaluator                                 #
    # ------------------------------------------------------------------ #
    def _eval_safe_expr(self, expr: str, df: pd.DataFrame,
                        feat_name: str) -> pd.Series:
        """
        Safely evaluate a math expression like:
            abs(${ATT.DesRoll} - ${ATT.Roll})
            max(${RCOU.C1}, ${RCOU.C2}, ${RCOU.C3}, ${RCOU.C4})

        Uses Python's ast module to parse the expression tree and evaluate
        it node-by-node against pandas Series. No eval() is used.

        Supported:
          - Column references: ${COL.NAME}
          - Arithmetic: +, -, *, /
          - Functions: abs(), max(), min(), sqrt()
          - Numeric literals
        """
        # Check all referenced columns exist
        refs = _COL_REF_PATTERN.findall(expr)
        missing = [r for r in refs if r not in df.columns]
        if missing:
            logger.warning("  %s → fallback references missing columns: %s",
                           feat_name, missing)
            return pd.Series(np.nan, index=df.index)

        # Replace ${COL.NAME} with safe placeholder variable names
        # e.g., ${ATT.Roll} → __ATT_Roll__
        col_map = {}  # placeholder_name → column_name
        safe_expr = expr
        for col in refs:
            placeholder = '__' + col.replace('.', '_') + '__'
            col_map[placeholder] = col
            safe_expr = safe_expr.replace('${' + col + '}', placeholder)

        # Parse the expression into an AST
        try:
            tree = ast.parse(safe_expr, mode='eval')
        except SyntaxError as e:
            logger.warning("  %s → failed to parse expression '%s': %s",
                           feat_name, safe_expr, e)
            return pd.Series(np.nan, index=df.index)

        # Evaluate the AST
        try:
            result = self._eval_node(tree.body, df, col_map)
            if isinstance(result, (int, float)):
                return pd.Series(result, index=df.index)
            return result
        except Exception as e:
            logger.warning("  %s → failed to evaluate expression: %s",
                           feat_name, e)
            return pd.Series(np.nan, index=df.index)

    def _eval_node(self, node, df: pd.DataFrame,
                   col_map: dict):
        """
        Recursively evaluate an AST node, returning a pandas Series or
        scalar.
        """
        # --- Numeric literal ---
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ValueError(f"Unsupported constant type: {type(node.value)}")

        # --- Variable name (column reference placeholder) ---
        if isinstance(node, ast.Name):
            placeholder = node.id
            if placeholder in col_map:
                return df[col_map[placeholder]]
            raise ValueError(f"Unknown variable: {placeholder}")

        # --- Unary operator: -x, +x ---
        if isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, df, col_map)
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.UAdd):
                return operand
            raise ValueError(f"Unsupported unary op: {type(node.op)}")

        # --- Binary operator: x + y, x - y, x * y, x / y ---
        if isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, df, col_map)
            right = self._eval_node(node.right, df, col_map)
            ops = {
                ast.Add: operator.add,
                ast.Sub: operator.sub,
                ast.Mult: operator.mul,
                ast.Div: operator.truediv,
            }
            op_func = ops.get(type(node.op))
            if op_func is None:
                raise ValueError(f"Unsupported binary op: {type(node.op)}")
            return op_func(left, right)

        # --- Function call: abs(), max(), min(), sqrt() ---
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ValueError("Only simple function calls are supported")

            func_name = node.func.id
            args = [self._eval_node(arg, df, col_map) for arg in node.args]

            if func_name == 'abs':
                if len(args) != 1:
                    raise ValueError("abs() requires exactly 1 argument")
                return np.abs(args[0])

            if func_name == 'sqrt':
                if len(args) != 1:
                    raise ValueError("sqrt() requires exactly 1 argument")
                return np.sqrt(args[0])

            if func_name == 'max':
                if len(args) < 2:
                    raise ValueError("max() requires at least 2 arguments")
                result = args[0]
                for a in args[1:]:
                    result = np.maximum(result, a)
                return result

            if func_name == 'min':
                if len(args) < 2:
                    raise ValueError("min() requires at least 2 arguments")
                result = args[0]
                for a in args[1:]:
                    result = np.minimum(result, a)
                return result

            raise ValueError(f"Unsupported function: {func_name}")

        raise ValueError(f"Unsupported AST node type: {type(node)}")
