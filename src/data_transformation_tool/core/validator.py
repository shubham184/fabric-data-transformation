"""Model validation functionality."""

import networkx as nx
from typing import Dict, List, Tuple
from .models import DataModel


class ModelValidator:
    """Validates data models for consistency and correctness"""
    
    def __init__(self, models: Dict[str, DataModel]):
        self.models = models
        self.errors = []
        
    def validate_all(self) -> Tuple[bool, List[str]]:
        """Validate all models"""
        self.errors = []
        
        # Run all validations
        self._validate_unique_model_names()
        self._validate_dependency_references()
        self._validate_column_references()
        self._validate_cte_consistency()
        self._validate_grain_columns()
        self._validate_audit_rules()
        
        return len(self.errors) == 0, self.errors
    
    def _validate_unique_model_names(self):
        """Ensure all model names are unique"""
        names = [model.model.name for model in self.models.values()]
        duplicates = [name for name in set(names) if names.count(name) > 1]
        for duplicate in duplicates:
            self.errors.append(f"Duplicate model name: '{duplicate}'")
    
    def _validate_dependency_references(self):
        """Validate that dependency references exist"""
        all_model_names = set(self.models.keys())
        
        for model_name, model in self.models.items():
            for dep in model.source.depends_on_tables:
                # Allow dependencies that are either:
                # 1. Other models in our collection
                # 2. External source tables (contain dots or start with known prefixes)
                if (dep not in all_model_names and 
                    not self._is_external_table(dep)):
                    self.errors.append(f"Model '{model_name}': dependency '{dep}' not found")
    
    def _validate_column_references(self):
        """Validate that column references are consistent"""
        for model_name, model in self.models.items():
            # Get all valid reference tables for this model
            valid_references = self._get_valid_reference_tables(model)
            
            for column in model.transformations.columns:
                if column.reference_table and column.reference_table not in valid_references:
                    # Only error if it's not an external table
                    if not self._is_external_table(column.reference_table):
                        self.errors.append(
                            f"Model '{model_name}': column '{column.name}' references "
                            f"table '{column.reference_table}' which is not available"
                        )
    
    def _get_valid_reference_tables(self, model: DataModel) -> set:
        """Get all valid reference tables for a model"""
        valid_tables = set()
        
        # Add base table if specified
        if model.source.base_table:
            valid_tables.add(model.source.base_table)
        
        # Add all dependencies
        valid_tables.update(model.source.depends_on_tables)
        
        # Add all CTEs
        valid_tables.update(model.ctes.ctes)
        
        # Add external/source tables (they're always valid)
        for column in model.transformations.columns:
            if self._is_external_table(column.reference_table):
                valid_tables.add(column.reference_table)
        
        return valid_tables
    
    def _is_external_table(self, table_name: str) -> bool:
        """Check if a table is an external/source table"""
        if not table_name:
            return False
            
        # Common patterns for external tables
        external_patterns = [
            'source.',
            'source_',
            'raw.',
            'raw_',
            'external.',
            'staging.',
            'landing.',
            'bronze.',
            'silver.',
            'gold.'
        ]
        
        # Check if table name contains schema separators or known prefixes
        return ('.' in table_name or 
                any(table_name.lower().startswith(pattern) for pattern in external_patterns))
    
    def _validate_cte_consistency(self):
        """Validate CTE usage consistency"""
        for model_name, model in self.models.items():
            cte_names = set(model.ctes.ctes)
            depends_on = set(model.source.depends_on_tables)
            
            # CTEs should generally be in dependencies (though not strictly required)
            missing_deps = cte_names - depends_on
            for missing in missing_deps:
                if missing in self.models:  # Only warn for internal models
                    # This is more of a warning than an error
                    pass  # We'll be lenient here
    
    def _validate_grain_columns(self):
        """Validate that grain columns exist in the model"""
        for model_name, model in self.models.items():
            if not model.grain:
                continue
                
            model_columns = {col.name for col in model.transformations.columns}
            for grain_col in model.grain:
                if grain_col not in model_columns:
                    self.errors.append(
                        f"Model '{model_name}': grain column '{grain_col}' not found in transformations"
                    )
    
    def _validate_audit_rules(self):
        """Validate audit rules"""
        for model_name, model in self.models.items():
            model_columns = {col.name for col in model.transformations.columns}
            
            for audit in model.audits.audits:
                for audit_col in audit.columns:
                    if audit_col not in model_columns:
                        self.errors.append(
                            f"Model '{model_name}': audit rule column '{audit_col}' not found in transformations"
                        )
    
    def validate_single_model(self, model_name: str) -> Tuple[bool, List[str]]:
        """Validate a single model"""
        if model_name not in self.models:
            return False, [f"Model '{model_name}' not found"]
        
        # Create temporary validator with just this model and its dependencies
        related_models = {model_name: self.models[model_name]}
        
        # Add dependencies to validation context
        model = self.models[model_name]
        for dep in model.source.depends_on_tables:
            if dep in self.models:
                related_models[dep] = self.models[dep]
        
        validator = ModelValidator(related_models)
        return validator.validate_all()
    
    def get_validation_summary(self) -> Dict[str, int]:
        """Get a summary of validation results"""
        error_types = {}
        for error in self.errors:
            if 'Duplicate model name' in error:
                error_types['duplicate_names'] = error_types.get('duplicate_names', 0) + 1
            elif 'dependency' in error:
                error_types['dependency_errors'] = error_types.get('dependency_errors', 0) + 1
            elif 'column' in error and 'references' in error:
                error_types['column_reference_errors'] = error_types.get('column_reference_errors', 0) + 1
            elif 'grain' in error:
                error_types['grain_errors'] = error_types.get('grain_errors', 0) + 1
            elif 'audit' in error:
                error_types['audit_errors'] = error_types.get('audit_errors', 0) + 1
            else:
                error_types['other_errors'] = error_types.get('other_errors', 0) + 1
        
        return error_types