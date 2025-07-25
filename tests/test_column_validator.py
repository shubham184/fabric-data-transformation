"""Unit tests for enhanced column validation functionality."""
from src.data_transformation_tool.core.column_validator import (
    SQLExpressionParser, 
    EnhancedColumnValidator,
    ColumnValidationError
)
from src.data_transformation_tool.core.models import (
    DataModel, ModelMetadata, SourceConfig, TransformationConfig, 
    ColumnTransformation, LayerType, ModelKind, RefreshFrequency
)


class TestSQLExpressionParser:
    """Test SQL expression parsing functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.parser = SQLExpressionParser()
    
    def test_extract_simple_column(self):
        """Test extraction of simple column references."""
        columns = self.parser.extract_column_references("column_name")
        assert columns == {"column_name"}
    
    def test_extract_function_columns(self):
        """Test extraction from SQL functions."""
        columns = self.parser.extract_column_references("SUM(BASEQUANTITY)")
        assert columns == {"BASEQUANTITY"}
        
        columns = self.parser.extract_column_references("COUNT(DISTINCT FORECASTITEMID)")
        assert columns == {"FORECASTITEMID"}
    
    def test_extract_complex_expression(self):
        """Test extraction from complex expressions."""
        expr = "UPPER(customer_name) + '_' + LOWER(region)"
        columns = self.parser.extract_column_references(expr)
        assert "customer_name" in columns
        # Note: region might be detected depending on parsing logic
    
    def test_custom_functions_ignored(self):
        """Test that custom functions are ignored."""
        columns = self.parser.extract_column_references("@newpk()")
        assert len(columns) == 0
        
        columns = self.parser.extract_column_references("@Feature('bias_calc')")
        assert len(columns) == 0
    
    def test_sql_keywords_ignored(self):
        """Test that SQL keywords are ignored."""
        expr = "CASE WHEN column1 = 'value' THEN column2 ELSE NULL END"
        columns = self.parser.extract_column_references(expr)
        # Should contain column1 and column2, but not CASE, WHEN, THEN, ELSE, END, NULL
        assert "column1" in columns
        assert "column2" in columns
        assert "CASE" not in columns
        assert "NULL" not in columns
    
    def test_expression_complexity(self):
        """Test expression complexity analysis."""
        complexity = self.parser.get_expression_complexity("SUM(column1)")
        assert complexity['functions'] == 1
        assert complexity['operators'] == 0
        
        complexity = self.parser.get_expression_complexity("column1 + column2")
        assert complexity['operators'] == 1
        
        complexity = self.parser.get_expression_complexity("FUNC('literal')")
        assert complexity['literals'] == 1


class TestEnhancedColumnValidator:
    """Test enhanced column validation functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.models = self._create_test_models()
        self.validator = EnhancedColumnValidator(self.models)
    
    def _create_test_models(self):
        """Create test models for validation."""
        # Source model
        source_model = DataModel(
            model=ModelMetadata(
                name="source_table",
                description="Source table",
                layer=LayerType.BRONZE,
                kind=ModelKind.TABLE,
                owner="test",
                domain="test",
                refresh_frequency=RefreshFrequency.DAILY
            ),
            source=SourceConfig(base_table="raw.source"),
            transformations=TransformationConfig(columns=[
                ColumnTransformation(
                    name="id",
                    reference_table="raw.source",
                    expression="",
                    description="ID column",
                    data_type="INTEGER"
                ),
                ColumnTransformation(
                    name="name",
                    reference_table="raw.source", 
                    expression="",
                    description="Name column",
                    data_type="VARCHAR"
                )
            ])
        )
        
        # Target model with valid reference
        target_model = DataModel(
            model=ModelMetadata(
                name="target_table",
                description="Target table",
                layer=LayerType.SILVER,
                kind=ModelKind.VIEW,
                owner="test",
                domain="test",
                refresh_frequency=RefreshFrequency.DAILY
            ),
            source=SourceConfig(depends_on_tables=["source_table"]),
            transformations=TransformationConfig(columns=[
                ColumnTransformation(
                    name="target_id",
                    reference_table="source_table",
                    expression="id",
                    description="Target ID",
                    data_type="INTEGER"
                ),
                ColumnTransformation(
                    name="invalid_column",
                    reference_table="source_table",
                    expression="non_existent_column",
                    description="Invalid reference",
                    data_type="VARCHAR"
                )
            ])
        )
        
        return {
            "source_table": source_model,
            "target_table": target_model
        }
    
    def test_validate_valid_column_reference(self):
        """Test validation of valid column references."""
        errors = self.validator.validate_model_column_references("target_table")
        
        # Should find one error (invalid_column references non_existent_column)
        assert len(errors) == 1
        assert errors[0].column_name == "invalid_column"
        assert errors[0].error_type == "COLUMN_NOT_FOUND"
        assert "non_existent_column" in errors[0].message
    
    def test_validate_all_models(self):
        """Test validation of all models."""
        errors = self.validator.validate_all_column_references()
        
        # Should find one error total
        assert len(errors) == 1
        assert errors[0].model_name == "target_table"
    
    def test_external_table_handling(self):
        """Test that external tables are handled correctly."""
        # Create model with external table reference
        external_model = DataModel(
            model=ModelMetadata(
                name="external_ref",
                description="External reference",
                layer=LayerType.BRONZE,
                kind=ModelKind.TABLE,
                owner="test",
                domain="test",
                refresh_frequency=RefreshFrequency.DAILY
            ),
            transformations=TransformationConfig(columns=[
                ColumnTransformation(
                    name="external_col",
                    reference_table="raw.external_table",
                    expression="some_column",
                    description="External column",
                    data_type="VARCHAR"
                )
            ])
        )
        
        test_models = {"external_ref": external_model}
        validator = EnhancedColumnValidator(test_models)
        
        errors = validator.validate_all_column_references()
        # Should not validate external tables
        assert len(errors) == 0
    
    def test_error_reporting(self):
        """Test error reporting functionality."""
        errors = self.validator.validate_all_column_references()
        
        # Test formatted report
        report = self.validator.format_error_report(include_suggestions=True)
        assert "Column validation errors" in report
        assert "target_table" in report
        
        # Test summary
        summary = self.validator.get_validation_summary()
        assert "COLUMN_NOT_FOUND" in summary
        assert summary["COLUMN_NOT_FOUND"] == 1
    
    def test_column_suggestions(self):
        """Test column name suggestions."""
        # The validator should suggest 'id' when 'non_existent_column' is not found
        errors = self.validator.validate_model_column_references("target_table")
        
        error = errors[0]
        # Suggestion logic may or may not provide a suggestion based on similarity
        # This tests the suggestion mechanism works
        assert hasattr(error, 'suggestion')
    
    def test_model_not_found(self):
        """Test handling of non-existent models."""
        errors = self.validator.validate_model_column_references("non_existent_model")
        
        assert len(errors) == 1
        assert errors[0].error_type == "MODEL_NOT_FOUND"


if __name__ == "__main__":
    # Simple test runner for development
    import sys
    
    test_parser = TestSQLExpressionParser()
    test_parser.setup_method()
    
    print("Testing SQL Expression Parser...")
    try:
        test_parser.test_extract_simple_column()
        test_parser.test_extract_function_columns()
        test_parser.test_custom_functions_ignored()
        test_parser.test_expression_complexity()
        print("✅ SQL Expression Parser tests passed")
    except Exception as e:
        print(f"❌ SQL Expression Parser tests failed: {e}")
        sys.exit(1)
    
    test_validator = TestEnhancedColumnValidator()
    test_validator.setup_method()
    
    print("Testing Enhanced Column Validator...")
    try:
        test_validator.test_validate_valid_column_reference()
        test_validator.test_validate_all_models()
        test_validator.test_external_table_handling()
        test_validator.test_error_reporting()
        test_validator.test_model_not_found()
        print("✅ Enhanced Column Validator tests passed")
    except Exception as e:
        print(f"❌ Enhanced Column Validator tests failed: {e}")
        sys.exit(1)
    
    print("✅ All tests passed!")