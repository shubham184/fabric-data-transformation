"""UNIQUE COMBINATION audit rule implementation."""

from typing import List
from .base_rule import BaseAuditRule
from ...core.models import DataModel


class UniqueCombinationRule(BaseAuditRule):
    """Audit rule for checking unique combinations of columns."""
    
    def get_rule_name(self) -> str:
        """Get the name of this rule type."""
        return "UNIQUE_COMBINATION"
    
    def generate_sql(self, model: DataModel) -> str:
        """Generate SQL for unique combination audit."""
        if not self.columns:
            raise ValueError("UNIQUE_COMBINATION rule requires at least one column")
        
        table_name = self._get_table_name(model)
        columns_list = ', '.join(self.columns)
        
        # Use a different approach for unique combination check
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  '{self.get_rule_name()}' as audit_type,
  '{columns_list}' as columns_checked,
  (COUNT(*) - COUNT(DISTINCT {columns_list})) as failed_rows,
  'Duplicate combinations found' as failure_condition,
  CURRENT_TIMESTAMP() as audit_timestamp
FROM {table_name}
HAVING COUNT(*) > COUNT(DISTINCT {columns_list})
"""
    
    def validate_rule_config(self, model: DataModel) -> List[str]:
        """Validate unique combination rule configuration."""
        errors = super().validate_rule_config(model)
        
        if not self.columns:
            errors.append("UNIQUE_COMBINATION rule requires at least one column")
        
        return errors
    
    def get_description(self) -> str:
        """Get a human-readable description of the rule."""
        if len(self.columns) == 1:
            return f"Checks that column '{self.columns[0]}' contains only unique values"
        else:
            return f"Checks that the combination of columns {', '.join(self.columns)} is unique"