"""
Plan feature implementation for the data transformation tool.
Similar to SQLMesh plan functionality.
"""

import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import logging


class ChangeType(Enum):
    """Types of changes that can occur to models"""
    NEW = "NEW"
    DELETED = "DELETED"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    LOGIC_CHANGE = "LOGIC_CHANGE"
    DEPENDENCY_CHANGE = "DEPENDENCY_CHANGE"
    METADATA_CHANGE = "METADATA_CHANGE"
    DOWNSTREAM_UPDATE = "DOWNSTREAM_UPDATE"


@dataclass
class ModelChange:
    """Represents a change to a model"""
    model_name: str
    change_type: ChangeType
    details: Dict[str, Any]
    directly_modified: bool = True


@dataclass
class StateComparison:
    """Results of comparing current state vs stored state"""
    new_models: Set[str]
    deleted_models: Set[str]
    modified_models: Dict[str, List[ModelChange]]
    indirectly_affected: Set[str]
    execution_order: List[str]


@dataclass
class ExecutionPlan:
    """Complete execution plan for applying changes"""
    environment: str
    changes: List[ModelChange]
    execution_order: List[str]
    summary: Dict[str, int]


class StateManager:
    """Manages environment-specific state files"""
    
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.state_dir = self.project_root / ".state"
        self.state_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def get_state_file_path(self, environment: str) -> Path:
        """Get path to state file for environment"""
        return self.state_dir / f"{environment}_state.json"
    
    def get_current_state(self, environment: str) -> Dict[str, Any]:
        """Load current state for environment"""
        state_file = self.get_state_file_path(environment)
        
        if not state_file.exists():
            self.logger.info(f"No state file found for {environment}, treating as empty state")
            return {}
        
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Error reading state file for {environment}: {e}")
            return {}
    
    def save_state(self, environment: str, models: Dict) -> None:
        """Save current state for environment"""
        state_file = self.get_state_file_path(environment)
        
        # Convert models to serializable state
        state_data = self._models_to_state(models)
        
        try:
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, sort_keys=True)
            self.logger.info(f"State saved for {environment} to {state_file}")
        except IOError as e:
            self.logger.error(f"Error saving state file for {environment}: {e}")
            raise
    
    def _models_to_state(self, models: Dict) -> Dict[str, Any]:
        """Convert models to serializable state representation"""
        state = {}
        
        for model_name, model in models.items():
            # Create a hash-based fingerprint of the model
            model_state = {
                'name': model_name,
                'schema_hash': self._compute_schema_hash(model),
                'logic_hash': self._compute_logic_hash(model),
                'metadata_hash': self._compute_metadata_hash(model),
                'dependencies': list(model.source.depends_on_tables) if hasattr(model.source, 'depends_on_tables') else [],
                'layer': model.model.layer.value if hasattr(model.model, 'layer') else None,
                'kind': model.model.kind.value if hasattr(model.model, 'kind') else None,
                'columns': self._extract_columns(model)
            }
            state[model_name] = model_state
        
        return state
    
    def _compute_schema_hash(self, model) -> str:
        """Compute hash of model schema (columns, types, etc.)"""
        schema_data = {
            'columns': self._extract_columns(model),
            'primary_keys': getattr(model.model, 'primary_keys', []),
            'unique_keys': getattr(model.model, 'unique_keys', [])
        }
        return self._hash_dict(schema_data)
    
    def _compute_logic_hash(self, model) -> str:
        """Compute hash of transformation logic"""
        logic_data = {}
        
        # Extract transformation logic
        if hasattr(model, 'transformations'):
            transformations = model.transformations
            if hasattr(transformations, 'sql'):
                logic_data['sql'] = transformations.sql
            if hasattr(transformations, 'columns'):
                logic_data['column_transforms'] = [
                    {
                        'name': col.name,
                        'expression': getattr(col, 'expression', None),
                        'type': getattr(col, 'type', None)
                    }
                    for col in transformations.columns
                ]
        
        return self._hash_dict(logic_data)
    
    def _compute_metadata_hash(self, model) -> str:
        """Compute hash of model metadata"""
        metadata = {
            'layer': model.model.layer.value if hasattr(model.model, 'layer') else None,
            'kind': model.model.kind.value if hasattr(model.model, 'kind') else None,
            'description': getattr(model.model, 'description', None),
            'tags': getattr(model.model, 'tags', [])
        }
        return self._hash_dict(metadata)
    
    def _extract_columns(self, model) -> List[Dict[str, Any]]:
        """Extract column information from model"""
        columns = []
        
        if hasattr(model, 'transformations') and hasattr(model.transformations, 'columns'):
            for col in model.transformations.columns:
                col_info = {
                    'name': col.name,
                    'type': getattr(col, 'type', None),
                    'nullable': getattr(col, 'nullable', True),
                    'description': getattr(col, 'description', None)
                }
                columns.append(col_info)
        
        return sorted(columns, key=lambda x: x['name'])
    
    def _hash_dict(self, data: Dict) -> str:
        """Create deterministic hash of dictionary"""
        json_str = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]


class ChangeDetector:
    """Detects changes between current models and stored state"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_changes(self, current_models: Dict, stored_state: Dict[str, Any]) -> List[ModelChange]:
        """Detect all changes between current and stored state"""
        changes = []
        
        current_model_names = set(current_models.keys())
        stored_model_names = set(stored_state.keys())
        
        # New models
        for model_name in current_model_names - stored_model_names:
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.NEW,
                details={'reason': 'Model added'},
                directly_modified=True
            ))
        
        # Deleted models
        for model_name in stored_model_names - current_model_names:
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.DELETED,
                details={'reason': 'Model removed'},
                directly_modified=True
            ))
        
        # Modified models
        for model_name in current_model_names & stored_model_names:
            model_changes = self._detect_model_changes(
                model_name, 
                current_models[model_name], 
                stored_state[model_name]
            )
            changes.extend(model_changes)
        
        return changes
    
    def _detect_model_changes(self, model_name: str, current_model, stored_state: Dict) -> List[ModelChange]:
        """Detect changes for a specific model"""
        changes = []
        state_manager = StateManager()
        
        # Get current model state
        current_state = {
            'schema_hash': state_manager._compute_schema_hash(current_model),
            'logic_hash': state_manager._compute_logic_hash(current_model),
            'metadata_hash': state_manager._compute_metadata_hash(current_model),
            'dependencies': list(current_model.source.depends_on_tables) if hasattr(current_model.source, 'depends_on_tables') else [],
            'columns': state_manager._extract_columns(current_model)
        }
        
        # Schema changes
        if current_state['schema_hash'] != stored_state.get('schema_hash'):
            schema_details = self._analyze_schema_changes(
                current_state['columns'], 
                stored_state.get('columns', [])
            )
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.SCHEMA_CHANGE,
                details=schema_details,
                directly_modified=True
            ))
        
        # Logic changes
        if current_state['logic_hash'] != stored_state.get('logic_hash'):
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.LOGIC_CHANGE,
                details={'reason': 'Transformation logic modified'},
                directly_modified=True
            ))
        
        # Dependency changes
        current_deps = set(current_state['dependencies'])
        stored_deps = set(stored_state.get('dependencies', []))
        if current_deps != stored_deps:
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.DEPENDENCY_CHANGE,
                details={
                    'added_dependencies': list(current_deps - stored_deps),
                    'removed_dependencies': list(stored_deps - current_deps)
                },
                directly_modified=True
            ))
        
        # Metadata changes
        if current_state.get('metadata_hash') != stored_state.get('metadata_hash'):
            changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.METADATA_CHANGE,
                details={'reason': 'Model metadata modified'},
                directly_modified=True
            ))
        
        return changes
    
    def _analyze_schema_changes(self, current_columns: List[Dict], stored_columns: List[Dict]) -> Dict[str, Any]:
        """Analyze detailed schema changes"""
        current_col_map = {col['name']: col for col in current_columns}
        stored_col_map = {col['name']: col for col in stored_columns}
        
        current_col_names = set(current_col_map.keys())
        stored_col_names = set(stored_col_map.keys())
        
        details = {
            'added_columns': list(current_col_names - stored_col_names),
            'removed_columns': list(stored_col_names - current_col_names),
            'modified_columns': []
        }
        
        # Check for modified columns
        for col_name in current_col_names & stored_col_names:
            current_col = current_col_map[col_name]
            stored_col = stored_col_map[col_name]
            
            if current_col != stored_col:
                modification = {'name': col_name}
                
                if current_col.get('type') != stored_col.get('type'):
                    modification['type_change'] = {
                        'from': stored_col.get('type'),
                        'to': current_col.get('type')
                    }
                
                if current_col.get('nullable') != stored_col.get('nullable'):
                    modification['nullable_change'] = {
                        'from': stored_col.get('nullable'),
                        'to': current_col.get('nullable')
                    }
                
                details['modified_columns'].append(modification)
        
        return details


class PlanGenerator:
    """Generates execution plans for applying changes"""
    
    def __init__(self, data_tool):
        self.data_tool = data_tool
        self.state_manager = StateManager()
        self.change_detector = ChangeDetector()
        self.logger = logging.getLogger(__name__)
    
    def generate_plan(self, models_directory: str, environment: str) -> ExecutionPlan:
        """Generate complete execution plan"""
        # Load current models
        self.data_tool.models = self.data_tool.yaml_reader.read_all_models()
        
        # Get stored state
        stored_state = self.state_manager.get_current_state(environment)
        
        # Detect changes
        direct_changes = self.change_detector.detect_changes(self.data_tool.models, stored_state)
        
        # Find indirectly affected models using lineage
        indirectly_affected = self._find_indirectly_affected(direct_changes)
        
        # Add downstream update changes
        all_changes = direct_changes.copy()
        for model_name in indirectly_affected:
            all_changes.append(ModelChange(
                model_name=model_name,
                change_type=ChangeType.DOWNSTREAM_UPDATE,
                details={'reason': 'Upstream dependency changed'},
                directly_modified=False
            ))
        
        # Generate execution order
        execution_order = self._generate_execution_order(all_changes)
        
        # Create summary
        summary = self._create_summary(all_changes)
        
        return ExecutionPlan(
            environment=environment,
            changes=all_changes,
            execution_order=execution_order,
            summary=summary
        )
    
    def _find_indirectly_affected(self, direct_changes: List[ModelChange]) -> Set[str]:
        """Find models indirectly affected by direct changes"""
        directly_modified_models = {
            change.model_name for change in direct_changes 
            if change.directly_modified and change.change_type != ChangeType.DELETED
        }
        
        indirectly_affected = set()
        
        try:
            # Use existing lineage functionality
            from ..lineage.graph_builder import LineageGraphBuilder
            graph_builder = LineageGraphBuilder(self.data_tool.models)
            
            for model_name in directly_modified_models:
                # Get downstream models using get_model_lineage method
                lineage_info = graph_builder.get_model_lineage(model_name)
                downstream = lineage_info.get('downstream_models', [])
                indirectly_affected.update(downstream)
                
        except ImportError:
            # Fallback to basic dependency analysis
            self.logger.warning("Enhanced lineage not available, using basic dependency analysis")
            indirectly_affected = self._basic_downstream_analysis(directly_modified_models)
        except Exception as e:
            # Fallback if there's any other error with lineage
            self.logger.warning(f"Error using enhanced lineage: {e}, falling back to basic analysis")
            indirectly_affected = self._basic_downstream_analysis(directly_modified_models)
        
        # Remove directly modified models from indirectly affected
        return indirectly_affected - directly_modified_models
    
    def _basic_downstream_analysis(self, modified_models: Set[str]) -> Set[str]:
        """Basic downstream analysis without enhanced lineage"""
        downstream = set()
        
        for model_name, model in self.data_tool.models.items():
            if hasattr(model.source, 'depends_on_tables'):
                dependencies = set(model.source.depends_on_tables)
                if dependencies & modified_models:
                    downstream.add(model_name)
        
        return downstream
    
    def _generate_execution_order(self, changes: List[ModelChange]) -> List[str]:
        """Generate execution order respecting dependencies"""
        try:
            # Use existing execution order logic
            execution_order = self.data_tool.get_execution_plan()
            
            # Filter to only include models that have changes
            changed_models = {change.model_name for change in changes if change.change_type != ChangeType.DELETED}
            filtered_order = [model for model in execution_order if model in changed_models]
            
            return filtered_order
            
        except Exception as e:
            self.logger.warning(f"Error generating execution order: {e}")
            # Fallback: return models in alphabetical order
            changed_models = [change.model_name for change in changes if change.change_type != ChangeType.DELETED]
            return sorted(changed_models)
    
    def _create_summary(self, changes: List[ModelChange]) -> Dict[str, int]:
        """Create summary statistics"""
        summary = {
            'new': 0,
            'deleted': 0,
            'directly_modified': 0,
            'indirectly_modified': 0,
            'total': len(changes)
        }
        
        for change in changes:
            if change.change_type == ChangeType.NEW:
                summary['new'] += 1
            elif change.change_type == ChangeType.DELETED:
                summary['deleted'] += 1
            elif change.change_type == ChangeType.DOWNSTREAM_UPDATE:
                summary['indirectly_modified'] += 1
            else:
                summary['directly_modified'] += 1
        
        return summary
    
    def apply_plan(self, plan: ExecutionPlan) -> bool:
        """Apply the execution plan (save new state)"""
        try:
            self.state_manager.save_state(plan.environment, self.data_tool.models)
            self.logger.info(f"Plan applied to {plan.environment} environment")
            return True
        except Exception as e:
            self.logger.error(f"Error applying plan: {e}")
            return False


class PlanFormatter:
    """Formats execution plans for display"""
    
    @staticmethod
    def format_plan(plan: ExecutionPlan) -> str:
        """Format plan in SQLMesh-like style"""
        output = []
        
        # Header
        output.append(f"Summary of changes for {plan.environment}:")
        output.append("Models:")
        
        # Group changes by type
        new_models = [c for c in plan.changes if c.change_type == ChangeType.NEW]
        deleted_models = [c for c in plan.changes if c.change_type == ChangeType.DELETED]
        modified_models = [c for c in plan.changes if c.directly_modified and c.change_type not in [ChangeType.NEW, ChangeType.DELETED]]
        indirect_models = [c for c in plan.changes if c.change_type == ChangeType.DOWNSTREAM_UPDATE]
        
        # Format sections
        if modified_models:
            output.append("├── Modified:")
            for i, change in enumerate(modified_models):
                prefix = "│   ├──" if i < len(modified_models) - 1 else "│   └──"
                details = PlanFormatter._format_change_details(change)
                output.append(f"{prefix} {change.model_name} ({details})")
        
        if new_models:
            output.append("├── New:")
            for i, change in enumerate(new_models):
                prefix = "│   ├──" if i < len(new_models) - 1 else "│   └──"
                output.append(f"{prefix} {change.model_name}")
        
        if deleted_models:
            output.append("├── Deleted:")
            for i, change in enumerate(deleted_models):
                prefix = "│   ├──" if i < len(deleted_models) - 1 else "│   └──"
                output.append(f"{prefix} {change.model_name}")
        
        if indirect_models:
            output.append("└── Indirectly Modified:")
            for i, change in enumerate(indirect_models):
                prefix = "    ├──" if i < len(indirect_models) - 1 else "    └──"
                # Find upstream cause
                upstream_cause = PlanFormatter._find_upstream_cause(change, plan.changes)
                output.append(f"{prefix} {change.model_name} (upstream: {upstream_cause})")
        
        # Summary stats
        output.append("")
        output.append(f"Directly Modified: {plan.summary['directly_modified'] + plan.summary['new']} models")
        output.append(f"Indirectly Modified: {plan.summary['indirectly_modified']} models")
        
        # Execution plan
        if plan.execution_order:
            output.append("")
            output.append("Execution Plan:")
            for i, model_name in enumerate(plan.execution_order, 1):
                change = next((c for c in plan.changes if c.model_name == model_name), None)
                change_label = f"[{change.change_type.value}]" if change else "[UNKNOWN]"
                output.append(f"{i:2d}. {model_name} {change_label}")
        
        return "\n".join(output)
    
    @staticmethod
    def _format_change_details(change: ModelChange) -> str:
        """Format change details for display"""
        if change.change_type == ChangeType.SCHEMA_CHANGE:
            details = change.details
            parts = []
            
            if details.get('added_columns'):
                parts.append(f"+{','.join(details['added_columns'])}")
            
            if details.get('removed_columns'):
                parts.append(f"-{','.join(details['removed_columns'])}")
            
            if details.get('modified_columns'):
                for mod in details['modified_columns']:
                    if 'type_change' in mod:
                        type_change = mod['type_change']
                        parts.append(f"~{mod['name']}:{type_change['from']}->{type_change['to']}")
                    else:
                        parts.append(f"~{mod['name']}")
            
            return f"Schema: {', '.join(parts)}" if parts else "Schema changes"
        
        elif change.change_type == ChangeType.LOGIC_CHANGE:
            return "Logic: transformation changed"
        
        elif change.change_type == ChangeType.DEPENDENCY_CHANGE:
            return "Dependencies: upstream references changed"
        
        elif change.change_type == ChangeType.METADATA_CHANGE:
            return "Metadata: model properties changed"
        
        return change.change_type.value.lower()
    
    @staticmethod
    def _find_upstream_cause(indirect_change: ModelChange, all_changes: List[ModelChange]) -> str:
        """Find what upstream change caused this indirect change"""
        # This is a simplified implementation
        # In a real scenario, you'd trace the dependency graph
        direct_changes = [c for c in all_changes if c.directly_modified and c.change_type != ChangeType.DELETED]
        if direct_changes:
            return direct_changes[0].model_name
        return "unknown"