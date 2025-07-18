"""Base class for data quality audit rules."""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from ...core.models import DataModel, AuditRule


class BaseAuditRule(ABC):
    """Base class for audit rule implementations."""
    
    def __init__(self, rule: AuditRule):
        self.rule = rule
        self.columns = rule.columns
        self.values = rule.values
    
    @abstractmethod
    def generate_sql(self, model: DataModel) -> str:
        """Generate SQL for this audit rule."""
        pass
    
    @abstractmethod
    def get_rule_name(self) -> str:
        """Get the name of this rule type."""
        pass
    
    def validate_rule_config(self, model: DataModel) -> List[str]:
        """Validate the rule configuration."""
        errors = []
        
        # Check if columns exist in the model
        model_columns = {col.name for col in model.transformations.columns}
        for column in self.columns:
            if column not in model_columns:
                errors.append(f"Audit rule column '{column}' not found in model '{model.model.name}'")
        
        return errors
    
    def _get_table_name(self, model: DataModel) -> str:
        """Get the full table name for the model."""
        return f"{model.model.layer.value}.{model.model.name}"
    
    def _format_audit_result(self, model: DataModel, failed_condition: str) -> str:
        """Format the audit result SQL."""
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  '{self.get_rule_name()}' as audit_type,
  '{', '.join(self.columns)}' as columns_checked,
  COUNT(*) as failed_rows,
  '{failed_condition}' as failure_condition,
  CURRENT_TIMESTAMP() as audit_timestamp
FROM {self._get_table_name(model)}
WHERE {failed_condition}
HAVING COUNT(*) > 0
"""
    
    def get_severity(self) -> str:
        """Get the severity level of this audit rule."""
        return "ERROR"  # Default severity
    
    def get_description(self) -> str:
        """Get a human-readable description of the rule."""
        return f"{self.get_rule_name()} check on columns: {', '.join(self.columns)}"