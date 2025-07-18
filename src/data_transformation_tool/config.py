import yaml
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class ToolConfig:
    """Configuration for the data transformation tool"""
    models_directory: str
    output_directory: str = "output"
    sql_dialect: str = "spark"
    generate_audits: bool = True
    generate_lineage: bool = True
    validate_on_read: bool = True
    log_level: str = "INFO"
    custom_templates: Optional[Dict[str, str]] = None
    incremental_strategy: str = "append"
    
    @classmethod
    def from_file(cls, config_file: str) -> 'ToolConfig':
        """Load configuration from YAML file"""
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'models_directory': self.models_directory,
            'output_directory': self.output_directory,
            'sql_dialect': self.sql_dialect,
            'generate_audits': self.generate_audits,
            'generate_lineage': self.generate_lineage,
            'validate_on_read': self.validate_on_read,
            'log_level': self.log_level,
            'custom_templates': self.custom_templates,
            'incremental_strategy': self.incremental_strategy
        }