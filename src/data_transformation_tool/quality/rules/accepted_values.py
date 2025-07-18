"""ACCEPTED VALUES audit rule implementation."""

from typing import List
from .base_rule import BaseAuditRule
from ...core.models import DataModel


class AcceptedValuesRule(BaseAuditRule):
    """Audit rule for checking accepted values."""
    
    def get_rule_name(self) -> str:
        """Get the name of this rule type."""
        return "ACCEPTED_VALUES"
    
    def generate_sql(self, model: DataModel) -> str:
        """Generate SQL for accepted values audit."""
        if not self.columns:
            raise ValueError("ACCEPTED_VALUES rule requires exactly one column")
        
        if len(self.columns) > 1:
            raise ValueError("ACCEPTED_VALUES rule supports only one column")
        
        if not self.values:
            raise ValueError("ACCEPTED_VALUES rule requires a list of accepted values")
        
        table_name = self._get_table_name(model)
        column = self.columns[0]
        
        # Create the accepted values list for SQL
        quoted_values = [f"'{value}'" for value in self.values]
        accepted_values_list = ', '.join(quoted_values)
        
        condition = f"{column} NOT IN ({accepted_values_list}) AND {column} IS NOT NULL"
        
        return self._format_audit_result(model, condition)
    
    def validate_rule_config(self, model: DataModel) -> List[str]:
        """Validate accepted values rule configuration."""
        errors = super().validate_rule_config(model)
        
        if not self.columns:
            errors.append("ACCEPTED_VALUES rule requires exactly one column")
        elif len(self.columns) > 1:
            errors.append("ACCEPTED_VALUES rule supports only one column")
        
        if not self.values:
            errors.append("ACCEPTED_VALUES rule requires a list of accepted values")
        
        return errors
    
    def get_description(self) -> str:
        """Get a human-readable description of the rule."""
        values_str = ', '.join([f"'{v}'" for v in self.values]) if self.values else "[]"
        return f"Checks that column '{self.columns[0]}' contains only accepted values: {values_str}"