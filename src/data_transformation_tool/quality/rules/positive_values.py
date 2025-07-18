"""POSITIVE VALUES audit rule implementation."""

from typing import List
from .base_rule import BaseAuditRule
from ...core.models import DataModel


class PositiveValuesRule(BaseAuditRule):
    """Audit rule for checking positive values."""
    
    def get_rule_name(self) -> str:
        """Get the name of this rule type."""
        return "POSITIVE_VALUES"
    
    def generate_sql(self, model: DataModel) -> str:
        """Generate SQL for positive values audit."""
        if not self.columns:
            raise ValueError("POSITIVE_VALUES rule requires at least one column")
        
        table_name = self._get_table_name(model)
        
        # Create conditions for each column (value <= 0 is a failure)
        negative_conditions = [f"({col} IS NOT NULL AND {col} <= 0)" for col in self.columns]
        combined_condition = " OR ".join(negative_conditions)
        
        return self._format_audit_result(model, combined_condition)
    
    def validate_rule_config(self, model: DataModel) -> List[str]:
        """Validate positive values rule configuration."""
        errors = super().validate_rule_config(model)
        
        if not self.columns:
            errors.append("POSITIVE_VALUES rule requires at least one column")
        
        # Check that columns are numeric types
        model_columns = {col.name: col.data_type for col in model.transformations.columns}
        numeric_types = {'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'INT', 'NUMERIC'}
        
        for column in self.columns:
            if column in model_columns:
                data_type = model_columns[column].upper()
                if not any(nt in data_type for nt in numeric_types):
                    errors.append(f"POSITIVE_VALUES rule column '{column}' should be numeric type, got '{data_type}'")
        
        return errors
    
    def get_description(self) -> str:
        """Get a human-readable description of the rule."""
        return f"Checks that columns {', '.join(self.columns)} contain only positive values (> 0)"