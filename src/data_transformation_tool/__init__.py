"""Data Transformation Tool - YAML to SQL converter for medallion architecture."""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .main import DataTransformationTool
from .config import ToolConfig

__all__ = ["DataTransformationTool", "ToolConfig"]
