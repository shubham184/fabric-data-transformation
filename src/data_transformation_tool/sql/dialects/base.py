"""Base SQL dialect class for extensibility."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any


class BaseSQLDialect(ABC):
    """Base class for SQL dialect implementations."""
    
    def __init__(self):
        self.name = self.__class__.__name__.lower().replace('dialect', '')
    
    @abstractmethod
    def format_table_name(self, schema: str, table: str) -> str:
        """Format table name for the dialect."""
        pass
    
    @abstractmethod
    def format_column_name(self, column: str) -> str:
        """Format column name for the dialect."""
        pass
    
    @abstractmethod
    def get_create_table_template(self) -> str:
        """Get CREATE TABLE template for the dialect."""
        pass
    
    @abstractmethod
    def get_create_view_template(self) -> str:
        """Get CREATE VIEW template for the dialect."""
        pass
    
    @abstractmethod
    def get_data_types_mapping(self) -> Dict[str, str]:
        """Get data type mapping for the dialect."""
        pass
    
    def format_partitioning(self, partition_columns: List[str]) -> str:
        """Format partitioning clause."""
        return ""
    
    def format_clustering(self, cluster_columns: List[str]) -> str:
        """Format clustering clause."""
        return ""
    
    def escape_string(self, value: str) -> str:
        """Escape string value for the dialect."""
        return f"'{value.replace(chr(39), chr(39) + chr(39))}'"  # Escape single quotes
    
    def format_comment(self, comment: str) -> str:
        """Format comment for the dialect."""
        return f"-- {comment}"
    
    def supports_merge(self) -> bool:
        """Check if dialect supports MERGE statements."""
        return False
    
    def supports_cte(self) -> bool:
        """Check if dialect supports Common Table Expressions."""
        return True
    
    def get_limit_clause(self, limit: int) -> str:
        """Get LIMIT clause for the dialect."""
        return f"LIMIT {limit}"