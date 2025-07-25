"""Main application class for the data transformation tool."""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .core.yaml_reader import YAMLReader
from .core.validator import ModelValidator
from .core.dependency_graph import DependencyGraph
from .sql.generator import SQLGenerator
from .config import ToolConfig
from .core.plan import PlanGenerator, PlanFormatter, StateManager


class DataTransformationTool:
    """Main application class orchestrating the entire tool"""
    
    def __init__(self, models_directory: str, config: Optional[Dict] = None):
        self.models_directory = Path(models_directory)
        self.config = config or {}
        self.models = {}
        self.sql_outputs = {}
        
        # Setup logging
        self._setup_logging()
        
        # Initialize components
        self.yaml_reader = YAMLReader(self.models_directory)
        self.validator = None
        self.sql_generator = None
        self.state_manager = None
        self.plan_generator = None
        
    def _setup_logging(self):
        """Configure logging"""
        log_level = self.config.get('log_level', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_pipeline(self) -> Tuple[bool, Dict[str, str], List[str]]:
        """Run the complete pipeline"""
        try:
            # Step 1: Read YAML files
            self.logger.info("Reading YAML configuration files...")
            self.models = self.yaml_reader.read_all_models()
            self.logger.info(f"Successfully read {len(self.models)} models")
            
            # Step 2: Validate models
            self.logger.info("Validating models...")
            self.validator = ModelValidator(self.models)
            is_valid, errors = self.validator.validate_all()
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(errors)} errors:")
                for error in errors:
                    self.logger.error(f"  - {error}")
                return False, {}, errors
            
            self.logger.info("All models validated successfully")
            
            # Step 3: Generate SQL
            self.logger.info("Generating SQL...")
            self.sql_generator = self._create_sql_generator()
            self.sql_outputs = self.sql_generator.generate_all_sql()
            self.logger.info(f"Generated SQL for {len(self.sql_outputs)} models")
            
            return True, self.sql_outputs, []
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False, {}, [str(e)]
    
    def _create_sql_generator(self):
        """Create SQL generator instance"""
        return SQLGenerator(self.models)
    
    def save_sql_outputs(self, output_directory: str):
        """Save generated SQL to files"""
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for model_name, sql in self.sql_outputs.items():
            sql_file = output_path / f"{model_name}.sql"
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(sql)
            
            self.logger.info(f"Saved SQL for {model_name} to {sql_file}")
    
    def get_lineage_report(self) -> str:
        """Generate basic lineage report"""
        lineage_data = {}
        for model_name, model in self.models.items():
            lineage_data[model_name] = {
                'name': model_name,
                'dependencies': model.source.depends_on_tables,
                'layer': model.model.layer.value,
                'kind': model.model.kind.value,
                'columns': [col.name for col in getattr(model.transformations, 'columns', [])]
            }
        
        import json
        return json.dumps(lineage_data, indent=2)
    
    def generate_html_lineage(self, output_directory: str) -> bool:
        """Generate interactive HTML lineage visualization"""
        # Skipping HTML generation for now as requested
        self.logger.info("HTML lineage generation skipped")
        return True
    
    def generate_dot_lineage(self, output_directory: str) -> bool:
        """Generate DOT lineage visualization"""
        try:
            # Use your enhanced graph builder
            from .lineage.graph_builder import LineageGraphBuilder
            
            # Build lineage graph
            graph_builder = LineageGraphBuilder(self.models)
            
            # Use the enhanced DOT export method (much cleaner)
            dot_content = graph_builder.export_dot_format(include_columns=False)
            
            # Save DOT file
            output_path = Path(output_directory)
            dot_file = output_path / "lineage.dot"
            
            with open(dot_file, 'w', encoding='utf-8') as f:
                f.write(dot_content)
            
            self.logger.info(f"DOT lineage saved to {dot_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error generating DOT lineage: {e}")
            return False
    
    def validate_specific_model(self, model_name: str) -> Tuple[bool, List[str]]:
        """Validate a specific model"""
        if model_name not in self.models:
            return False, [f"Model '{model_name}' not found"]
        
        # Create a validator for just this model
        single_model = {model_name: self.models[model_name]}
        validator = ModelValidator(single_model)
        return validator.validate_all()
    
    def get_execution_plan(self) -> List[str]:
        """Get the execution order for all models"""
        if not self.sql_generator:
            self.sql_generator = self._create_sql_generator()
        
        return self.sql_generator.dependency_graph.get_execution_order()
    
    def get_column_lineage(self, model_name: str, column_name: str) -> Dict:
        """Get column lineage using enhanced graph builder"""
        try:
            from .lineage.graph_builder import LineageGraphBuilder
            graph_builder = LineageGraphBuilder(self.models)
            return graph_builder.get_column_lineage_detailed(model_name, column_name)
        except Exception as e:
            self.logger.error(f"Error getting column lineage: {e}")
            return {}
    
    def get_enhanced_lineage_data(self) -> Dict:
        """Get enhanced lineage data with column-level information"""
        try:
            from .lineage.graph_builder import LineageGraphBuilder
            graph_builder = LineageGraphBuilder(self.models)
            return graph_builder.export_graph_data()
        except Exception as e:
            self.logger.error(f"Error getting enhanced lineage data: {e}")
            return {}
    
    def generate_full_lineage_reports(self, output_directory: str) -> bool:
        """Generate all lineage reports: JSON, HTML, and DOT formats"""
        try:
            from .lineage.graph_builder import LineageGraphBuilder
            from .lineage.exporters.json_exporter import JSONLineageExporter
            from .lineage.exporters.html_exporter import HTMLLineageExporter
            from .lineage.exporters.dot_exporter import DOTLineageExporter
            
            # Ensure models are loaded
            if not self.models:
                self.models = self.yaml_reader.read_all_models()
            
            # Create output directory
            output_path = Path(output_directory)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Initialize graph builder
            graph_builder = LineageGraphBuilder(self.models)
            
            # 1. Generate enhanced JSON lineage
            self.logger.info("Generating enhanced JSON lineage report...")
            json_exporter = JSONLineageExporter(graph_builder)
            enhanced_json = json_exporter.export_full_lineage()
            
            enhanced_json_file = output_path / "enhanced_lineage_report.json"
            with open(enhanced_json_file, 'w', encoding='utf-8') as f:
                f.write(enhanced_json)
            self.logger.info(f"Enhanced JSON lineage saved to {enhanced_json_file}")
            
            # 2. Generate interactive HTML lineage
            self.logger.info("Generating interactive HTML lineage visualization...")
            html_exporter = HTMLLineageExporter(graph_builder)
            html_content = html_exporter.export_interactive_lineage("Data Lineage Visualization")
            
            html_file = output_path / "interactive_lineage.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"Interactive HTML lineage saved to {html_file}")
            
            # 3. Generate DOT lineage files
            self.logger.info("Generating DOT lineage diagrams...")
            dot_exporter = DOTLineageExporter(graph_builder)
            
            # Model-level lineage
            model_dot = dot_exporter.export_model_lineage(include_columns=False)
            model_dot_file = output_path / "lineage.dot"
            with open(model_dot_file, 'w', encoding='utf-8') as f:
                f.write(model_dot)
            self.logger.info(f"Model-level DOT lineage saved to {model_dot_file}")
            
            # Column-level lineage
            column_dot = dot_exporter.export_model_lineage(include_columns=True)
            column_dot_file = output_path / "column_lineage.dot"
            with open(column_dot_file, 'w', encoding='utf-8') as f:
                f.write(column_dot)
            self.logger.info(f"Column-level DOT lineage saved to {column_dot_file}")
            
            # Also generate the basic lineage report for compatibility
            lineage_report = self.get_lineage_report()
            lineage_file = output_path / "lineage_report.json"
            with open(lineage_file, 'w', encoding='utf-8') as f:
                f.write(lineage_report)
            self.logger.info(f"Basic lineage report saved to {lineage_file}")
            
            print(f"📊 Generated full lineage reports:")
            print(f"  • enhanced_lineage_report.json - Detailed column-level lineage")
            print(f"  • interactive_lineage.html - Interactive visualization")
            print(f"  • lineage.dot - Model-level DOT diagram")
            print(f"  • column_lineage.dot - Column-level DOT diagram")
            print(f"  • lineage_report.json - Basic lineage report")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error generating full lineage reports: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    def initialize_plan_components(self):
        """Initialize plan-related components"""
        if not self.state_manager:
            self.state_manager = StateManager()
        if not self.plan_generator:
            self.plan_generator = PlanGenerator(self)
    
    def generate_plan(self, environment: str):
        """Generate execution plan for environment"""
        self.initialize_plan_components()
        return self.plan_generator.generate_plan(self.models_directory, environment)
    
    def apply_plan(self, plan):
        """Apply execution plan"""
        self.initialize_plan_components()
        return self.plan_generator.apply_plan(plan)
    
    def init_environment_state(self, environment: str) -> bool:
        """Initialize state for environment with current models"""
        try:
            # Ensure models are loaded
            if not self.models:
                self.models = self.yaml_reader.read_all_models()
            
            # Validate models before saving state
            self.validator = ModelValidator(self.models)
            is_valid, errors = self.validator.validate_all()
            
            if not is_valid:
                self.logger.error(f"Cannot initialize state with invalid models:")
                for error in errors:
                    self.logger.error(f"  - {error}")
                return False
            
            # Initialize state manager and save
            self.initialize_plan_components()
            self.state_manager.save_state(environment, self.models)
            self.logger.info(f"State initialized for {environment} with {len(self.models)} models")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing state: {e}")
            return False
    
    def get_environment_state(self, environment: str) -> Dict:
        """Get current state for environment"""
        self.initialize_plan_components()
        return self.state_manager.get_current_state(environment)
    
    def validate_and_plan(self, environment: str):
        """Validate models and generate plan"""
        try:
            # Step 1: Read and validate models
            self.logger.info("Reading and validating models...")
            self.models = self.yaml_reader.read_all_models()
            
            self.validator = ModelValidator(self.models)
            is_valid, errors = self.validator.validate_all()
            
            if not is_valid:
                self.logger.error("Validation failed:")
                for error in errors:
                    self.logger.error(f"  - {error}")
                return False, None, errors
            
            # Step 2: Generate plan
            self.logger.info(f"Generating plan for {environment}...")
            plan = self.generate_plan(environment)
            
            return True, plan, []
            
        except Exception as e:
            self.logger.error(f"Error in validate_and_plan: {e}")
            return False, None, [str(e)]
    
    def run_pipeline_with_plan(self, environment: str, auto_apply: bool = False):
        """Run pipeline with plan validation"""
        try:
            # Generate plan first
            success, plan, errors = self.validate_and_plan(environment)
            
            if not success:
                return False, {}, errors
            
            # Display plan
            formatted_plan = PlanFormatter.format_plan(plan)
            self.logger.info(f"Execution plan:\n{formatted_plan}")
            
            # Check if there are changes
            if not plan.changes:
                self.logger.info("No changes detected. Environment is up to date.")
                return True, {}, []
            
            # Apply plan if requested
            if auto_apply:
                self.logger.info(f"Auto-applying plan to {environment}...")
                if not self.apply_plan(plan):
                    return False, {}, ["Failed to apply plan"]
            
            # Run the actual pipeline
            success, sql_outputs, errors = self.run_pipeline()
            
            if success and not auto_apply:
                # Ask if user wants to save state
                self.logger.info("Pipeline completed successfully. Consider applying plan to save state.")
            
            return success, sql_outputs, errors
            
        except Exception as e:
            self.logger.error(f"Pipeline with plan failed: {str(e)}")
            return False, {}, [str(e)]
    
    def compare_environments(self, source_env: str, target_env: str):
        """Compare state between two environments"""
        self.initialize_plan_components()
        
        source_state = self.state_manager.get_current_state(source_env)
        target_state = self.state_manager.get_current_state(target_env)
        
        if not source_state:
            self.logger.warning(f"No state found for {source_env}")
            return {}
        
        if not target_state:
            self.logger.warning(f"No state found for {target_env}")
            return {}
        
        # Find differences
        source_models = set(source_state.keys())
        target_models = set(target_state.keys())
        
        comparison = {
            'source_only': source_models - target_models,
            'target_only': target_models - source_models,
            'common': source_models & target_models,
            'differences': {}
        }
        
        # Check differences in common models
        for model_name in comparison['common']:
            source_model = source_state[model_name]
            target_model = target_state[model_name]
            
            diffs = []
            if source_model.get('schema_hash') != target_model.get('schema_hash'):
                diffs.append('schema')
            if source_model.get('logic_hash') != target_model.get('logic_hash'):
                diffs.append('logic')
            if source_model.get('metadata_hash') != target_model.get('metadata_hash'):
                diffs.append('metadata')
            if set(source_model.get('dependencies', [])) != set(target_model.get('dependencies', [])):
                diffs.append('dependencies')
            
            if diffs:
                comparison['differences'][model_name] = diffs
        
        return comparison
    
    def promote_to_environment(self, source_env: str, target_env: str, auto_apply: bool = False):
        """Promote changes from source environment to target environment"""
        try:
            self.logger.info(f"Promoting changes from {source_env} to {target_env}...")
            
            # Load current models (should match source_env state)
            self.models = self.yaml_reader.read_all_models()
            
            # Generate plan for target environment
            plan = self.generate_plan(target_env)
            
            # Display promotion plan
            formatted_plan = PlanFormatter.format_plan(plan)
            self.logger.info(f"Promotion plan:\n{formatted_plan}")
            
            if not plan.changes:
                self.logger.info(f"{target_env} is already up to date with {source_env}")
                return True
            
            # Apply if requested
            if auto_apply:
                success = self.apply_plan(plan)
                if success:
                    self.logger.info(f"Successfully promoted changes to {target_env}")
                else:
                    self.logger.error(f"Failed to promote changes to {target_env}")
                return success
            else:
                self.logger.info(f"Use --auto-apply to promote changes to {target_env}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error promoting to {target_env}: {e}")
            return False
    
    def get_plan_summary(self, environment: str) -> str:
        """Get a summary of what would change"""
        try:
            plan = self.generate_plan(environment)
            
            if not plan.changes:
                return f"✅ {environment} environment is up to date"
            
            summary = plan.summary
            lines = [
                f"📋 Plan summary for {environment}:",
                f"  New models: {summary['new']}",
                f"  Directly modified: {summary['directly_modified']}",  
                f"  Indirectly affected: {summary['indirectly_modified']}",
                f"  Total changes: {summary['total']}"
            ]
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ Error generating plan summary: {e}"