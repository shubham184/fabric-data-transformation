"""Spark SQL dialect implementation."""

from typing import Dict, List
from .base import BaseSQLDialect


class SparkSQLDialect(BaseSQLDialect):
    """Spark SQL dialect implementation."""
    
    def format_table_name(self, schema: str, table: str) -> str:
        """Format table name for Spark SQL."""
        return f"{schema}.{table}"
    
    def format_column_name(self, column: str) -> str:
        """Format column name for Spark SQL."""
        # Spark allows most column names without escaping
        if ' ' in column or '-' in column or column.startswith('_'):
            return f"`{column}`"
        return column
    
    def get_create_table_template(self) -> str:
        """Get CREATE TABLE template for Spark SQL."""
        return """CREATE TABLE {{ schema }}.{{ table_name }}
{%- if partitioned_by %}
USING DELTA
PARTITIONED BY ({{ partitioned_by | join(", ") }})
{%- endif %}
{%- if clustered_by %}
CLUSTERED BY ({{ clustered_by | join(", ") }})
{%- endif %}
AS
{%- if ctes %}
WITH
{{ cte_sql }}
{%- endif %}
{{ select_sql }}"""
    
    def get_create_view_template(self) -> str:
        """Get CREATE VIEW template for Spark SQL."""
        return """CREATE VIEW {{ schema }}.{{ table_name }} AS
{%- if ctes %}
WITH
{{ cte_sql }}
{%- endif %}
{{ select_sql }}"""
    
    def get_data_types_mapping(self) -> Dict[str, str]:
        """Get Spark SQL data type mapping."""
        return {
            'STRING': 'STRING',
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'DOUBLE',
            'DECIMAL': 'DECIMAL',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP',
            'ARRAY': 'ARRAY',
            'MAP': 'MAP',
            'STRUCT': 'STRUCT',
            'BINARY': 'BINARY'
        }
    
    def format_partitioning(self, partition_columns: List[str]) -> str:
        """Format Spark SQL partitioning clause."""
        if partition_columns:
            return f"PARTITIONED BY ({', '.join(partition_columns)})"
        return ""
    
    def format_clustering(self, cluster_columns: List[str]) -> str:
        """Format Spark SQL clustering clause."""
        if cluster_columns:
            return f"CLUSTERED BY ({', '.join(cluster_columns)})"
        return ""
    
    def supports_merge(self) -> bool:
        """Spark SQL supports MERGE statements."""
        return True
    
    def get_merge_template(self) -> str:
        """Get MERGE statement template for Spark SQL."""
        return """MERGE INTO {{ target_table }} AS target
USING ({{ source_query }}) AS source
ON {{ merge_condition }}
WHEN MATCHED THEN
  UPDATE SET {{ update_set_clause }}
WHEN NOT MATCHED THEN
  INSERT ({{ insert_columns }})
  VALUES ({{ insert_values }})"""
    
    def get_optimize_hint(self, hint_type: str, columns: List[str] = None) -> str:
        """Get optimization hints for Spark SQL."""
        if hint_type == "broadcast":
            return "/*+ BROADCAST */"
        elif hint_type == "merge" and columns:
            return f"/*+ MERGE({', '.join(columns)}) */"
        elif hint_type == "shuffle_hash" and columns:
            return f"/*+ SHUFFLE_HASH({', '.join(columns)}) */"
        return ""