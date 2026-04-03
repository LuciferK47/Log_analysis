"""
abstraction.py — Version-Agnostic Feature Extraction Layer (DuckDB SQL Generation)

Core innovation of the proposal. Consumes feature_registry.yaml and dynamically
generates DuckDB CREATE VIEW statements for the abstract features.
Uses SQL COALESCE statements for falling back to computed expressions.
"""
import logging
import re
import yaml

logger = logging.getLogger(__name__)

_COL_REF_PATTERN = re.compile(r'\$\{([^}]+)\}')


class FeatureExtractor:
    """Extracts version-agnostic features using DuckDB dynamic SQL views."""

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def get_tables_from_expr(self, expr: str) -> set:
        """Extract table names from ${TABLE.COLUMN} references."""
        cols = _COL_REF_PATTERN.findall(expr)
        return {c.split('.')[0] for c in cols}

    def compute_features(self, con) -> None:
        """
        Translates YAML registry rules into DuckDB views using COALESCE.
        Creates a 'feat_<name>' view for each abstract feature.
        """
        logger.info("Computing SQL views for abstract features...")

        # Get existing raw tables/views mounted from Parquet
        try:
            existing_tables_df = con.execute("SHOW TABLES").df()
            existing_tables = existing_tables_df['name'].tolist() if not existing_tables_df.empty else []
        except Exception:
            existing_tables = []

        for feat_name, rules in self.config.get('features', {}).items():
            primary = rules.get('priority_1')
            fallback = rules.get('fallback')

            tables = set()
            sql_primary = "NULL"
            sql_fallback = "NULL"

            if primary:
                parts = primary.split('.')
                if len(parts) == 2:
                    table, col = parts
                    tables.add(table)
                    sql_primary = f"{table}.{col}"

            if fallback:
                expr = fallback
                cols = _COL_REF_PATTERN.findall(fallback)
                for c in cols:
                    parts = c.split('.')
                    if len(parts) == 2:
                        table, col = parts
                        tables.add(table)
                        # Replaces ${TABLE.COL} with TABLE.col for SQL evaluation
                        expr = expr.replace(f"${{{c}}}", f"{table}.{col}")
                sql_fallback = expr

            # Filter to existing tables
            tables = {t for t in tables if t in existing_tables}

            if not tables:
                logger.warning("Skipping feature %s: no required tables found.", feat_name)
                # Create empty view
                con.execute(f"CREATE OR REPLACE VIEW feat_{feat_name} AS SELECT NULL AS TimeUS, NULL AS value WHERE 1=0")
                continue

            tables_list = list(tables)
            base_table = tables_list[0]

            if len(tables_list) == 1:
                from_clause = f"FROM {base_table}"
                time_col = f"{base_table}.TimeUS"
            else:
                from_clause = f"FROM {base_table}"
                time_cols = [f"{base_table}.TimeUS"]
                for t in tables_list[1:]:
                    from_clause += f" FULL OUTER JOIN {t} ON {base_table}.TimeUS = {t}.TimeUS"
                    time_cols.append(f"{t}.TimeUS")
                time_col = f"COALESCE({', '.join(time_cols)})"

            query = f"""
            CREATE OR REPLACE VIEW feat_{feat_name} AS
            SELECT 
                {time_col} AS TimeUS,
                COALESCE({sql_primary}, {sql_fallback}) AS value
            {from_clause}
            """
            try:
                con.execute(query)
                logger.debug("Created VIEW feat_%s", feat_name)
            except Exception as e:
                logger.error("Failed to create VIEW feat_%s: %s", feat_name, e)

        # Create a single spine view with all features for easy querying? Optional.
        return con
