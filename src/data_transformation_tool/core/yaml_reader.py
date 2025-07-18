"""YAML file reading functionality."""

import yaml
from pathlib import Path
from typing import Any, Dict
from .models import DataModel
from .exceptions import YAMLParsingError

class YAMLReader:
    """Reads and parses YAML model configuration files"""
    
    def __init__(self, models_directory: Path):
        self.models_directory = Path(models_directory)
        self.example_config = self._load_example_config()
        
        # Files to skip - these are not model definitions
        self.skip_files = {
            'config.yaml', 'config.yml',
            'settings.yaml', 'settings.yml',
            'dbt_project.yml', 'dbt_project.yaml',
            'profiles.yml', 'profiles.yaml',
            'example_model_config.yaml'  # Skip our example file
        }
    
    def _load_example_config(self) -> dict:
        """Load the example configuration file"""
        example_path = Path(__file__).parent / 'example_model_config.yaml'
        
        try:
            with open(example_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not load example config: {e}")
            return {}
    
    def read_all_models(self) -> Dict[str, DataModel]:
        """Read all model YAML files from the directory"""
        models = {}
        yaml_files = list(self.models_directory.rglob('*.yaml')) + list(self.models_directory.rglob('*.yml'))
        
        for yaml_file in yaml_files:
            # Skip configuration files
            if yaml_file.name.lower() in self.skip_files:
                print(f"⏭️  Skipping config file: {yaml_file}")
                continue
                
            # Skip hidden files and directories
            if any(part.startswith('.') for part in yaml_file.parts):
                continue
            
            try:
                model = self._read_single_model(yaml_file)
                models[model.model.name] = model
                print(f"✅ Loaded model: {model.model.name} from {yaml_file}")
            except YAMLParsingError as e:
                print(f"{str(e)}")
                raise ValueError(f"Format error in {yaml_file.name}")
            except Exception as e:
                raise ValueError(f"Error reading {yaml_file}: {str(e)}")
        
        return models
    
    def _read_single_model(self, file_path: Path) -> DataModel:
        """Read and parse a single YAML model file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = yaml.safe_load(f)
            
            if not isinstance(raw_data, dict):
                raise ValueError(f"YAML file must contain a dictionary, got {type(raw_data)}")
            
            if 'model' not in raw_data:
                raise ValueError("YAML file must contain a 'model' section")
            
            return DataModel.parse_obj(raw_data)
            
        except yaml.YAMLError as e:
            raise YAMLParsingError(f"Invalid YAML syntax in {file_path}: {str(e)}")
        except Exception as e:
            enhanced_error = self._create_helpful_error(raw_data, e, file_path)
            raise YAMLParsingError(enhanced_error)
    
    def _create_helpful_error(self, raw_data: dict, pydantic_error: Exception, file_path: Path) -> str:
        """Create helpful error message using example config"""
        error_str = str(pydantic_error)
        
        # Detect which section has the error
        section_name = self._detect_error_section(error_str, raw_data)
        
        if section_name and section_name in self.example_config:
            return self._format_section_error(section_name, raw_data, file_path.name)
        
        # Fallback for unknown errors
        return f"Validation error in {file_path}: {error_str}"
    
    def _detect_error_section(self, error_str: str, raw_data: dict) -> str:
        """Detect which section has the validation error"""
        
        # Check for specific patterns in order of priority
        section_patterns = [
            ('audits', ['audits']),
            ('transformations', ['transformations', 'columns']),
            ('relationships', ['relationships', 'foreign_keys']),
            ('filters', ['filters', 'where_conditions']),
            ('aggregations', ['aggregations', 'group_by', 'having']),
            ('ctes', ['ctes']),
            ('optimization', ['optimization', 'partitioned_by', 'clustered_by']),
            ('source', ['source', 'depends_on_tables']),
            ('model', ['model', 'name', 'layer', 'kind'])
        ]
        
        for section_name, keywords in section_patterns:
            if any(keyword in error_str for keyword in keywords):
                return section_name
        
        return None
    
    def _format_section_error(self, section_name: str, raw_data: dict, filename: str) -> str:
        """Format error message showing correct example for the section"""
        
        # Get the example for this section
        example_section = self.example_config.get(section_name, {})
        
        # Convert to YAML string
        section_yaml = yaml.dump({section_name: example_section}, default_flow_style=False, sort_keys=False)
        
        # Detect what they actually provided
        actual_format = self._describe_actual_format(section_name, raw_data)
        
        return (
            f"❌ Incorrect '{section_name}' format in {filename}\n\n"
            f"✅ Expected format:\n{section_yaml}"
            f"❌ You provided:{actual_format}\n"
            f"💡 Check data types, required fields, and structure. Copy expected format above."
        )
    
    def _describe_actual_format(self, section_name: str, raw_data: dict) -> str:
        """Describe what the user actually provided"""
        if section_name not in raw_data:
            return f"Missing '{section_name}' section"
        
        # Show their actual YAML structure
        actual_data = raw_data[section_name]
        try:
            actual_yaml = yaml.dump({section_name: actual_data}, default_flow_style=False, sort_keys=False)
            return f"\n{actual_yaml}"
        except:
            # Fallback if YAML dump fails
            if isinstance(actual_data, list):
                return f"List format (should be object)"
            elif isinstance(actual_data, dict):
                return f"Object with keys: {list(actual_data.keys())}"
            else:
                return f"{type(actual_data).__name__} (should be object)"
    
    
    def _get_section_tips(self, section_name: str, field_type: Any) -> str:
        """Get helpful tips for a specific section"""
        
        # Get field definition from DataModel
        field_info = DataModel.__fields__.get(section_name)
        if not field_info:
            return "Check the documentation for correct format"
        
        tips = []
        
        # Required vs optional
        is_required = self._is_field_required(field_info)
        if is_required:
            tips.append(f"'{section_name}' section is required")
        else:
            tips.append(f"'{section_name}' section is optional")
        
        # Type-specific tips
        if hasattr(field_type, '__fields__'):
            required_fields = []
            for name, info in field_type.__fields__.items():
                if self._is_field_required(info):
                    required_fields.append(name)
            if required_fields:
                tips.append(f"Required fields: {', '.join(required_fields)}")
        
        return " | ".join(tips)
    
    def get_model_file_path(self, model_name: str) -> Path:
        """Get the file path for a specific model"""
        for yaml_file in self.models_directory.rglob('*.yaml'):
            if yaml_file.name.lower() in self.skip_files:
                continue
                
            try:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    raw_data = yaml.safe_load(f)
                
                if isinstance(raw_data, dict) and 'model' in raw_data:
                    if raw_data['model'].get('name') == model_name:
                        return yaml_file
            except:
                continue
        
        raise FileNotFoundError(f"Model '{model_name}' not found")
    
    def validate_yaml_structure(self, file_path: Path) -> bool:
        """Validate that a YAML file has the correct structure for a model"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = yaml.safe_load(f)
            
            if not isinstance(raw_data, dict):
                return False
            
            # Check for required top-level sections
            required_sections = ['model']
            for section in required_sections:
                if section not in raw_data:
                    return False
            
            # Check model section has required fields
            model_section = raw_data['model']
            required_model_fields = ['name', 'layer', 'kind']
            for field in required_model_fields:
                if field not in model_section:
                    return False
            
            return True
            
        except Exception:
            return False