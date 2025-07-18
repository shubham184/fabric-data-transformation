"""Custom exceptions for the data transformation tool."""


class DataTransformationError(Exception):
    """Base exception for data transformation tool."""
    pass


class ValidationError(DataTransformationError):
    """Raised when model validation fails."""
    
    def __init__(self, message: str, errors: list = None):
        super().__init__(message)
        self.errors = errors or []


class SQLGenerationError(DataTransformationError):
    """Raised when SQL generation fails."""
    pass


class DependencyError(DataTransformationError):
    """Raised when dependency resolution fails."""
    pass


class ConfigurationError(DataTransformationError):
    """Raised when configuration is invalid."""
    pass


class YAMLParsingError(DataTransformationError):
    """Raised when YAML parsing fails."""
    pass


class LineageError(DataTransformationError):
    """Raised when lineage tracking fails."""
    pass


class AuditError(DataTransformationError):
    """Raised when audit generation fails."""
    pass


class IncrementalProcessingError(DataTransformationError):
    """Raised when incremental processing fails."""
    pass