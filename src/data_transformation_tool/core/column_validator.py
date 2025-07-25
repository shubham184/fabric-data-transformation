"""Enhanced column-level validation utilities for data transformation models."""

import re
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
from .models import DataModel, ColumnTransformation


@dataclass
class ColumnValidationError:
    """Represents a column validation error with detailed context."""
    model_name: str
    column_name: str
    error_type: str
    message: str
    reference_table: Optional[str] = None
    referenced_column: Optional[str] = None
    available_columns: Optional[List[str]] = None
    suggestion: Optional[str] = None


class SQLExpressionParser:
    """Parses SQL expressions to extract column references."""
    
    # SQL keywords and functions that should be ignored
    SQL_KEYWORDS = {
        'select', 'from', 'where', 'group', 'by', 'order', 'having',
        'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not',
        'in', 'exists', 'between', 'like', 'is', 'null', 'distinct',
        'as', 'on', 'inner', 'left', 'right', 'full', 'outer', 'join',
        'union', 'intersect', 'except', 'with', 'recursive'
    }
    
    # SQL functions that don't reference columns
    SQL_FUNCTIONS = {
        'count', 'sum', 'avg', 'min', 'max', 'std', 'var',
        'concat', 'substring', 'length', 'upper', 'lower', 'trim',
        'cast', 'convert', 'coalesce', 'nullif', 'isnull',
        'year', 'month', 'day', 'datepart', 'format', 'getdate',
        'newid', 'rand', 'abs', 'round', 'ceiling', 'floor',
        'row_number', 'rank', 'dense_rank', 'lead', 'lag',
        'first_value', 'last_value'
    }
    
    # Custom functions that don't reference actual columns
    CUSTOM_FUNCTIONS = {
        '@newpk', '@feature', '@config'
    }
    
    def __init__(self):
        """Initialize the SQL expression parser."""
        # Pattern to match potential column names (alphanumeric + underscore)
        self.column_pattern = re.compile(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b')
        
        # Pattern to match function calls
        self.function_pattern = re.compile(r'\b([a-zA-Z_@][a-zA-Z0-9_]*)\s*\(')
        
        # Pattern to match string literals
        self.string_pattern = re.compile(r"'[^']*'|\"[^\"]*\"")
        
        # Pattern to match numeric literals
        self.numeric_pattern = re.compile(r'\b\d+\.?\d*\b')
    
    def extract_column_references(self, expression: str) -> Set[str]:
        """
        Extract column references from a SQL expression.
        
        Args:
            expression: The SQL expression to parse
            
        Returns:
            Set of column names referenced in the expression
        """
        if not expression or not expression.strip():
            return set()
        
        # Handle special custom functions
        if self._is_custom_function(expression):
            return set()
        
        # Clean the expression
        cleaned_expr = self._clean_expression(expression)
        
        # Extract potential column names
        potential_columns = set()
        
        # Find all word-like tokens
        for match in self.column_pattern.finditer(cleaned_expr):
            token = match.group().lower()
            
            # Skip SQL keywords
            if token in self.SQL_KEYWORDS:
                continue
                
            # Skip if it's a function call
            if self._is_function_call(expression, match.start()):
                continue
                
            # Skip if it's a SQL function
            if token in self.SQL_FUNCTIONS:
                continue
                
            potential_columns.add(match.group())  # Keep original case
        
        return potential_columns
    
    def _clean_expression(self, expression: str) -> str:
        """Clean expression by removing string literals and comments."""
        # Remove string literals
        cleaned = self.string_pattern.sub('', expression)
        
        # Remove single-line comments
        cleaned = re.sub(r'--.*$', '', cleaned, flags=re.MULTILINE)
        
        # Remove multi-line comments
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        
        return cleaned
    
    def _is_custom_function(self, expression: str) -> bool:
        """Check if expression is a custom function call."""
        expr_lower = expression.lower().strip()
        return any(func in expr_lower for func in self.CUSTOM_FUNCTIONS)
    
    def _is_function_call(self, expression: str, position: int) -> bool:
        """Check if the token at position is part of a function call."""
        # Look ahead for opening parenthesis
        remaining = expression[position:]
        match = re.match(r'[a-zA-Z_][a-zA-Z0-9_]*\s*\(', remaining)
        return match is not None
    
    def get_expression_complexity(self, expression: str) -> Dict[str, int]:
        """Analyze expression complexity for better error reporting."""
        if not expression:
            return {'functions': 0, 'operators': 0, 'literals': 0}
        
        function_matches = self.function_pattern.findall(expression)
        operator_matches = re.findall(r'[+\-*/=<>!]+', expression)
        literal_matches = self.string_pattern.findall(expression) + self.numeric_pattern.findall(expression)
        
        return {
            'functions': len(function_matches),
            'operators': len(operator_matches),
            'literals': len(literal_matches)
        }


class EnhancedColumnValidator:
    """Enhanced validator for column-level references and dependencies."""
    
    def __init__(self, models: Dict[str, DataModel]):
        """
        Initialize the enhanced column validator.
        
        Args:
            models: Dictionary of model name to DataModel objects
        """
        self.models = models
        self.parser = SQLExpressionParser()
        self.errors: List[ColumnValidationError] = []
        
        # Build column mapping for quick lookups
        self.model_columns: Dict[str, Set[str]] = {}
        self._build_column_mappings()
    
    def _build_column_mappings(self) -> None:
        """Build a mapping of model names to their available columns."""
        for model_name, model in self.models.items():
            self.model_columns[model_name] = {
                col.name for col in model.transformations.columns
            }
    
    def validate_all_column_references(self) -> List[ColumnValidationError]:
        """
        Validate all column references across all models.
        
        Returns:
            List of column validation errors found
        """
        self.errors = []
        
        for model_name, model in self.models.items():
            self._validate_model_column_references(model_name, model)
        
        return self.errors
    
    def validate_model_column_references(self, model_name: str) -> List[ColumnValidationError]:
        """
        Validate column references for a specific model.
        
        Args:
            model_name: Name of the model to validate
            
        Returns:
            List of column validation errors for this model
        """
        if model_name not in self.models:
            return [ColumnValidationError(
                model_name=model_name,
                column_name="",
                error_type="MODEL_NOT_FOUND",
                message=f"Model '{model_name}' not found"
            )]
        
        model_errors = []
        model = self.models[model_name]
        
        for column in model.transformations.columns:
            column_errors = self._validate_column_reference(model_name, column)
            model_errors.extend(column_errors)
        
        return model_errors
    
    def _validate_model_column_references(self, model_name: str, model: DataModel) -> None:
        """Validate column references for a single model."""
        for column in model.transformations.columns:
            column_errors = self._validate_column_reference(model_name, column)
            self.errors.extend(column_errors)
    
    def _validate_column_reference(self, model_name: str, column: ColumnTransformation) -> List[ColumnValidationError]:
        """
        Validate a single column reference.
        
        Args:
            model_name: Name of the model containing the column
            column: The column transformation to validate
            
        Returns:
            List of validation errors for this column
        """
        errors = []
        
        # Skip validation for external tables
        if not column.reference_table or self._is_external_table(column.reference_table):
            return errors
        
        # Check if reference table exists
        if column.reference_table not in self.models:
            errors.append(ColumnValidationError(
                model_name=model_name,
                column_name=column.name,
                error_type="REFERENCE_TABLE_NOT_FOUND",
                message=f"Referenced table '{column.reference_table}' not found",
                reference_table=column.reference_table,
                suggestion=self._suggest_similar_table(column.reference_table)
            ))
            return errors
        
        # Get available columns in reference table
        available_columns = self.model_columns.get(column.reference_table, set())
        
        # Determine what columns are being referenced
        referenced_columns = self._get_referenced_columns(column)
        
        # Validate each referenced column
        for ref_column in referenced_columns:
            if ref_column not in available_columns:
                # Skip if it's a special function or SQL construct
                if self._is_sql_construct(ref_column, column.expression or ""):
                    continue
                
                errors.append(ColumnValidationError(
                    model_name=model_name,
                    column_name=column.name,
                    error_type="COLUMN_NOT_FOUND",
                    message=f"Column '{ref_column}' not found in table '{column.reference_table}'",
                    reference_table=column.reference_table,
                    referenced_column=ref_column,
                    available_columns=sorted(list(available_columns)),
                    suggestion=self._suggest_similar_column(ref_column, available_columns)
                ))
        
        return errors
    
    def _get_referenced_columns(self, column: ColumnTransformation) -> Set[str]:
        """Get all columns referenced by a column transformation."""
        if column.expression and column.expression.strip():
            # Parse expression to find column references
            return self.parser.extract_column_references(column.expression)
        else:
            # Direct column reference (column name mapping)
            return {column.name}
    
    def _is_external_table(self, table_name: str) -> bool:
        """Check if a table is an external/source table."""
        if not table_name:
            return False
            
        external_patterns = [
            'source.', 'source_', 'raw.', 'raw_', 'external.',
            'staging.', 'landing.', 'bronze.', 'silver.', 'gold.'
        ]
        
        return ('.' in table_name or 
                any(table_name.lower().startswith(pattern) for pattern in external_patterns))
    
    def _is_sql_construct(self, token: str, expression: str) -> bool:
        """Check if a token is a SQL construct that shouldn't be validated."""
        token_lower = token.lower()
        
        # Check if it's a SQL keyword
        if token_lower in self.parser.SQL_KEYWORDS:
            return True
        
        # Check if it's a SQL function
        if token_lower in self.parser.SQL_FUNCTIONS:
            return True
        
        # Check if it's part of a custom function
        if any(func in expression.lower() for func in self.parser.CUSTOM_FUNCTIONS):
            return True
        
        # Check if it's a numeric literal
        if re.match(r'^\d+\.?\d*$', token):
            return True
        
        # Check if it's a string literal
        if token.startswith(("'", '"')) and token.endswith(("'", '"')):
            return True
        
        return False
    
    def _suggest_similar_table(self, table_name: str) -> Optional[str]:
        """Suggest a similar table name if available."""
        if not table_name:
            return None
        
        table_lower = table_name.lower()
        best_match = None
        best_score = 0
        
        for model_name in self.models.keys():
            model_lower = model_name.lower()
            
            # Simple similarity based on common substrings
            common_chars = sum(1 for a, b in zip(table_lower, model_lower) if a == b)
            score = common_chars / max(len(table_lower), len(model_lower))
            
            if score > best_score and score > 0.5:  # 50% similarity threshold
                best_score = score
                best_match = model_name
        
        return best_match
    
    def _suggest_similar_column(self, column_name: str, available_columns: Set[str]) -> Optional[str]:
        """Suggest a similar column name if available."""
        if not column_name or not available_columns:
            return None
        
        column_lower = column_name.lower()
        best_match = None
        best_score = 0
        
        for available_col in available_columns:
            available_lower = available_col.lower()
            
            # Exact match (case insensitive)
            if column_lower == available_lower:
                return available_col
            
            # Substring match
            if column_lower in available_lower or available_lower in column_lower:
                return available_col
            
            # Simple similarity based on common characters
            common_chars = sum(1 for a, b in zip(column_lower, available_lower) if a == b)
            score = common_chars / max(len(column_lower), len(available_lower))
            
            if score > best_score and score > 0.6:  # 60% similarity threshold
                best_score = score
                best_match = available_col
        
        return best_match
    
    def get_validation_summary(self) -> Dict[str, int]:
        """Get a summary of validation results by error type."""
        summary = {}
        for error in self.errors:
            summary[error.error_type] = summary.get(error.error_type, 0) + 1
        return summary
    
    def get_errors_by_model(self) -> Dict[str, List[ColumnValidationError]]:
        """Group validation errors by model name."""
        errors_by_model = {}
        for error in self.errors:
            if error.model_name not in errors_by_model:
                errors_by_model[error.model_name] = []
            errors_by_model[error.model_name].append(error)
        return errors_by_model
    
    def format_error_report(self, include_suggestions: bool = True) -> str:
        """Format validation errors into a readable report."""
        if not self.errors:
            return "✅ No column validation errors found."
        
        report_lines = [
            f"❌ Found {len(self.errors)} column validation errors:",
            "=" * 60
        ]
        
        errors_by_model = self.get_errors_by_model()
        
        for model_name, model_errors in errors_by_model.items():
            report_lines.append(f"\n📋 Model: {model_name}")
            report_lines.append("-" * 40)
            
            for error in model_errors:
                report_lines.append(f"  🔴 Column '{error.column_name}': {error.message}")
                
                if error.available_columns and len(error.available_columns) <= 10:
                    cols_str = ", ".join(error.available_columns)
                    report_lines.append(f"     Available columns: {cols_str}")
                elif error.available_columns:
                    cols_str = ", ".join(error.available_columns[:5])
                    report_lines.append(f"     Available columns: {cols_str}... (+{len(error.available_columns)-5} more)")
                
                if include_suggestions and error.suggestion:
                    report_lines.append(f"     💡 Suggestion: Use '{error.suggestion}'")
                
                report_lines.append("")
        
        # Add summary
        summary = self.get_validation_summary()
        report_lines.extend([
            "📊 Error Summary:",
            "-" * 20
        ])
        
        for error_type, count in summary.items():
            report_lines.append(f"  {error_type}: {count}")
        
        return "\n".join(report_lines)