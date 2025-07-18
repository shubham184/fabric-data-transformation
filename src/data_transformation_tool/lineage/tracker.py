from typing import Dict, List, Optional, Union
from ..core.models import DataModel, ColumnTransformation
from ..core.dependency_graph import DependencyGraph

# Import will be done dynamically to avoid circular imports

class LineageTracker:
    """Tracks data lineage across models with flexible backend support"""
    
    def __init__(self, models: Dict[str, DataModel], use_enhanced_lineage: bool = True):
        self.models = models
        self.use_enhanced_lineage = use_enhanced_lineage
        
        # Initialize both backends for flexibility
        self.dependency_graph = DependencyGraph(models)
        
        if use_enhanced_lineage:
            try:
                from .graph_builder import LineageGraphBuilder
                self.graph_builder = LineageGraphBuilder(models)
            except ImportError:
                self.graph_builder = None
        else:
            self.graph_builder = None
    
    def get_column_lineage(self, model_name: str, column_name: str) -> Dict:
        """Trace lineage for a specific column"""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        model = self.models[model_name]
        column = self._find_column(model, column_name)
        
        if not column:
            raise ValueError(f"Column '{column_name}' not found in model '{model_name}'")
        
        if self.use_enhanced_lineage and self.graph_builder:
            return self._get_enhanced_column_lineage(model_name, column_name, column)
        else:
            return self._get_basic_column_lineage(model_name, column_name, column)
    
    def _get_enhanced_column_lineage(self, model_name: str, column_name: str, column) -> Dict:
        """Get column lineage using enhanced graph builder"""
        detailed_lineage = self.graph_builder.get_column_lineage_detailed(model_name, column_name)
        
        # Convert to standard format for backward compatibility
        lineage = {
            'model': model_name,
            'column': column_name,
            'source_table': column.reference_table,
            'expression': column.expression or column_name,
            'upstream_dependencies': [],
            'enhanced_data': {
                'transformation_paths': detailed_lineage['transformation_path'],
                'all_upstream': detailed_lineage['all_upstream_columns'],
                'all_downstream': detailed_lineage['all_downstream_columns']
            }
        }
        
        # Convert detailed upstream to standard format
        for upstream in detailed_lineage['upstream_columns']:
            upstream_lineage = {
                'model': upstream['model'],
                'column': upstream['column'],
                'source_table': upstream['model'],
                'expression': upstream.get('expression') or upstream['column'],
                'transformation_type': upstream.get('transformation_type', 'unknown'),
                'upstream_dependencies': []
            }
            lineage['upstream_dependencies'].append(upstream_lineage)
        
        return lineage
    
    def _get_basic_column_lineage(self, model_name: str, column_name: str, column, visited: Optional[set] = None) -> Dict:
        """Get column lineage using basic recursive approach with cycle protection"""
        if visited is None:
            visited = set()
        
        # Prevent infinite recursion
        current_key = f"{model_name}.{column_name}"
        if current_key in visited:
            return {
                'model': model_name,
                'column': column_name,
                'source_table': column.reference_table,
                'expression': column.expression or column_name,
                'upstream_dependencies': [],
                'cycle_detected': True
            }
        
        visited.add(current_key)
        
        lineage = {
            'model': model_name,
            'column': column_name,
            'source_table': column.reference_table,
            'expression': column.expression or column_name,
            'upstream_dependencies': []
        }
        
        # Trace upstream dependencies
        if column.reference_table and column.reference_table in self.models:
            try:
                upstream_column_name = column.expression or column_name
                upstream_model = self.models[column.reference_table]
                upstream_column = self._find_column(upstream_model, upstream_column_name)
                
                if upstream_column:
                    upstream_lineage = self._get_basic_column_lineage(
                        column.reference_table, 
                        upstream_column_name,
                        upstream_column,
                        visited.copy()  # Pass a copy to allow parallel branches
                    )
                    lineage['upstream_dependencies'].append(upstream_lineage)
            except Exception:
                # Handle any parsing errors gracefully
                pass
        
        return lineage
    
    def get_column_lineage_detailed(self, model_name: str, column_name: str) -> Dict:
        """Get detailed column lineage (requires enhanced lineage)"""
        if not self.use_enhanced_lineage or not self.graph_builder:
            raise ValueError("Enhanced lineage is required for detailed column lineage. "
                           "Initialize with use_enhanced_lineage=True")
        
        return self.graph_builder.get_column_lineage_detailed(model_name, column_name)
    
    def get_column_impact_analysis(self, model_name: str, column_name: str) -> Dict:
        """Analyze impact of column changes (requires enhanced lineage)"""
        if not self.use_enhanced_lineage or not self.graph_builder:
            raise ValueError("Enhanced lineage is required for impact analysis. "
                           "Initialize with use_enhanced_lineage=True")
        
        return self.graph_builder.get_column_impact_analysis(model_name, column_name)
    
    def get_model_lineage(self, model_name: str) -> Dict:
        """Get complete lineage for a model"""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        model = self.models[model_name]
        
        if self.use_enhanced_lineage and self.graph_builder:
            return self._get_enhanced_model_lineage(model_name, model)
        else:
            return self._get_basic_model_lineage(model_name, model)
    
    def _get_enhanced_model_lineage(self, model_name: str, model: DataModel) -> Dict:
        """Get model lineage using enhanced graph builder"""
        model_lineage = self.graph_builder.get_model_lineage(model_name)
        
        # Enhanced column information
        columns_info = []
        for col in model.transformations.columns:
            col_lineage = self.graph_builder.get_column_lineage_detailed(model_name, col.name)
            columns_info.append({
                'name': col.name,
                'source_table': col.reference_table,
                'expression': col.expression,
                'data_type': getattr(col, 'data_type', 'unknown'),
                'upstream_columns': len(col_lineage['upstream_columns']),
                'downstream_columns': len(col_lineage['downstream_columns']),
                'transformation_path_count': len(col_lineage['transformation_path'])
            })
        
        return {
            'model': model_name,
            'layer': model.model.layer.value,
            'kind': model.model.kind.value,
            'direct_dependencies': model.source.depends_on_tables,
            'all_dependencies': model_lineage['all_upstream'],
            'dependents': model_lineage['all_downstream'],
            'execution_order_position': self._get_execution_position(model_name),
            'lineage_depth': model_lineage['lineage_depth'],
            'columns': columns_info,
            'total_columns': len(model.transformations.columns),
            'has_circular_dependencies': self._check_circular_dependencies(model_name)
        }
    
    def _get_basic_model_lineage(self, model_name: str, model: DataModel) -> Dict:
        """Get model lineage using basic dependency graph"""
        dependencies = self.dependency_graph.get_dependencies(model_name)
        dependents = self.dependency_graph.get_dependents(model_name)
        
        return {
            'model': model_name,
            'layer': model.model.layer.value,
            'kind': model.model.kind.value,
            'direct_dependencies': model.source.depends_on_tables,
            'all_dependencies': list(dependencies),
            'dependents': list(dependents),
            'execution_order_position': self._get_execution_position(model_name),
            'columns': [
                {
                    'name': col.name,
                    'source_table': col.reference_table,
                    'expression': col.expression,
                    'data_type': getattr(col, 'data_type', 'unknown')
                }
                for col in model.transformations.columns
            ],
            'total_columns': len(model.transformations.columns),
            'has_circular_dependencies': self.dependency_graph.has_circular_dependencies()
        }
    
    def get_execution_order(self) -> List[str]:
        """Get topologically sorted execution order"""
        if self.use_enhanced_lineage and self.graph_builder:
            return self.graph_builder.get_execution_order()
        else:
            return self.dependency_graph.get_execution_order()
    
    def get_impact_analysis(self, model_name: str) -> Dict:
        """Get impact analysis for model changes"""
        if self.use_enhanced_lineage and self.graph_builder:
            return self.graph_builder.get_impact_analysis(model_name)
        else:
            # Basic impact analysis using dependency graph
            dependents = self.dependency_graph.get_dependents(model_name)
            return {
                'impacted_models': list(dependents),
                'total_impacted_models': len(dependents),
                'impact_by_layer': self._group_by_layer(dependents),
                'critical_models': []  # Not available in basic mode
            }
    
    def find_circular_dependencies(self) -> Dict[str, List]:
        """Find circular dependencies"""
        if self.use_enhanced_lineage and self.graph_builder:
            return {
                'model_cycles': self.graph_builder.find_circular_dependencies(),
                'column_cycles': self.graph_builder.find_circular_column_dependencies()
            }
        else:
            return {
                'model_cycles': self.dependency_graph.find_circular_dependencies(),
                'column_cycles': []  # Not available in basic mode
            }
    
    def get_lineage_statistics(self) -> Dict:
        """Get comprehensive lineage statistics"""
        if self.use_enhanced_lineage and self.graph_builder:
            return {
                'dependency_stats': self.graph_builder.get_dependency_statistics(),
                'layer_stats': self.graph_builder.get_layer_statistics(),
                'execution_order': self.get_execution_order(),
                'circular_dependencies': self.find_circular_dependencies(),
                'backend': 'enhanced'
            }
        else:
            return {
                'dependency_stats': self.dependency_graph.get_graph_statistics(),
                'execution_order': self.get_execution_order(),
                'circular_dependencies': self.find_circular_dependencies(),
                'backend': 'basic'
            }
    
    def _find_column(self, model: DataModel, column_name: str) -> Optional[ColumnTransformation]:
        """Find a column definition in a model"""
        for col in model.transformations.columns:
            if col.name == column_name:
                return col
        return None
    
    def _get_execution_position(self, model_name: str) -> int:
        """Get the position of a model in the execution order"""
        try:
            execution_order = self.get_execution_order()
            return execution_order.index(model_name) + 1
        except (ValueError, AttributeError):
            return -1
    
    def _check_circular_dependencies(self, model_name: str) -> bool:
        """Check if a model is part of any circular dependency"""
        cycles = self.find_circular_dependencies()
        model_cycles = cycles.get('model_cycles', [])
        
        for cycle in model_cycles:
            if model_name in cycle:
                return True
        return False
    
    def _group_by_layer(self, model_names: set) -> Dict:
        """Group models by layer"""
        groups = {}
        for model_name in model_names:
            if model_name in self.models:
                layer = self.models[model_name].model.layer.value
                if layer not in groups:
                    groups[layer] = []
                groups[layer].append(model_name)
        return groups
    
    def export_lineage_graph(self, output_format: str = 'json', include_columns: bool = False) -> str:
        """Export complete lineage graph"""
        if output_format == 'json':
            if include_columns and self.use_enhanced_lineage and self.graph_builder:
                return self._export_enhanced_json()
            else:
                return self._export_basic_json()
        elif output_format == 'dot':
            if include_columns and self.use_enhanced_lineage and self.graph_builder:
                return self.graph_builder.export_dot_format(include_columns=True)
            else:
                # Use basic dependency graph for DOT export
                return self.dependency_graph.export_dot()
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _export_basic_json(self) -> str:
        """Export in basic JSON format"""
        import json
        lineage_data = {}
        
        for model_name in self.models:
            lineage_data[model_name] = self.get_model_lineage(model_name)
        
        return json.dumps(lineage_data, indent=2)
    
    def _export_enhanced_json(self) -> str:
        """Export enhanced JSON with column-level lineage"""
        import json
        
        graph_data = self.graph_builder.export_graph_data()
        
        enhanced_data = {
            'metadata': {
                'total_models': len(self.models),
                'total_columns': sum(len(model.transformations.columns) for model in self.models.values()),
                'execution_order': self.get_execution_order(),
                'has_circular_dependencies': len(self.find_circular_dependencies()['model_cycles']) > 0,
                'backend': 'enhanced'
            },
            'lineage': graph_data,
            'statistics': self.get_lineage_statistics()
        }
        
        return json.dumps(enhanced_data, indent=2)
    
    # Enhanced-only methods (graceful fallback)
    def trace_column_to_source(self, model_name: str, column_name: str) -> List[Dict]:
        """Trace a column back to its ultimate source(s)"""
        if not self.use_enhanced_lineage or not self.graph_builder:
            # Fallback to basic recursive tracing
            lineage = self.get_column_lineage(model_name, column_name)
            return self._extract_sources_from_basic_lineage(lineage)
        
        lineage = self.get_column_lineage_detailed(model_name, column_name)
        sources = []
        for path in lineage['transformation_path']:
            if path['path']:
                source_step = path['path'][0]
                sources.append({
                    'source_model': source_step['model'],
                    'source_column': source_step['column'],
                    'path_length': len(path['path']),
                    'transformations': [step.get('transformation_type', 'unknown') 
                                      for step in path['path'][1:]]
                })
        return sources
    
    def _extract_sources_from_basic_lineage(self, lineage: Dict, sources: Optional[List] = None) -> List[Dict]:
        """Extract source columns from basic lineage recursively"""
        if sources is None:
            sources = []
        
        if not lineage.get('upstream_dependencies'):
            # This is a source
            sources.append({
                'source_model': lineage['model'],
                'source_column': lineage['column'],
                'path_length': 1,
                'transformations': []
            })
        else:
            # Recurse into upstream dependencies
            for upstream in lineage['upstream_dependencies']:
                self._extract_sources_from_basic_lineage(upstream, sources)
        
        return sources
    
    def find_columns_from_source(self, source_model: str, source_column: str) -> List[Dict]:
        """Find all columns that derive from a specific source column"""
        if not self.use_enhanced_lineage or not self.graph_builder:
            raise ValueError("Enhanced lineage is required for this operation. "
                           "Initialize with use_enhanced_lineage=True")
        
        column_id = f"{source_model}.{source_column}"
        
        if column_id not in self.graph_builder.column_graph:
            return []
        
        import networkx as nx
        descendants = list(nx.descendants(self.graph_builder.column_graph, column_id))
        
        derived_columns = []
        for desc_id in descendants:
            desc_model, desc_column = desc_id.split('.', 1)
            derived_columns.append({
                'model': desc_model,
                'column': desc_column,
                'column_id': desc_id
            })
        
        return derived_columns
    
    # Convenience methods
    def switch_backend(self, use_enhanced_lineage: bool):
        """Switch between basic and enhanced lineage backends"""
        self.use_enhanced_lineage = use_enhanced_lineage
        if use_enhanced_lineage and not self.graph_builder:
            try:
                from .graph_builder import LineageGraphBuilder
                self.graph_builder = LineageGraphBuilder(self.models)
            except ImportError:
                self.graph_builder = None
    
    def get_backend_info(self) -> Dict:
        """Get information about the current backend"""
        return {
            'backend_type': 'enhanced' if self.use_enhanced_lineage else 'basic',
            'column_lineage_available': self.use_enhanced_lineage and self.graph_builder is not None,
            'features': {
                'basic_model_lineage': True,
                'enhanced_model_lineage': self.use_enhanced_lineage,
                'column_lineage': self.use_enhanced_lineage,
                'column_impact_analysis': self.use_enhanced_lineage,
                'transformation_paths': self.use_enhanced_lineage,
                'circular_dependency_detection': True
            }
        }