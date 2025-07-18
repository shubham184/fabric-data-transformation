"""NOT NULL audit rule implementation."""

from typing import List
from .base_rule import BaseAuditRule
from ...core.models import DataModel


class NotNullRule(BaseAuditRule):
    """Audit rule for checking NOT NULL constraints."""
    
    def get_rule_name(self) -> str:
        """Get the name of this rule type."""
        return "NOT_NULL"
    
    def generate_sql(self, model: DataModel) -> str:
        """Generate SQL for NOT NULL audit."""
        if not self.columns:
            raise ValueError("NOT NULL rule requires at least one column")
        
        table_name = self._get_table_name(model)
        
        # Create conditions for each column
        null_conditions = [f"{col} IS NULL" for col in self.columns]
        combined_condition = " OR ".join(null_conditions)
        
        return self._format_audit_result(model, combined_condition)
    
    def validate_rule_config(self, model: DataModel) -> List[str]:
        """Validate NOT NULL rule configuration."""
        errors = super().validate_rule_config(model)
        
        if not self.columns:
            errors.append("NOT NULL rule requires at least one column")
        
        return errors
    
    def get_description(self) -> str:
        """Get a human-readable description of the rule."""
        return f"Checks that columns {', '.join(self.columns)} contain no NULL values"