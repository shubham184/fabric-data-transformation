"""Core lineage graph builder for data transformation models."""

import networkx as nx
import re
from typing import Dict, List, Set, Any, Tuple, Optional
from ..core.models import DataModel


class LineageGraphBuilder:
    """Builds and manages lineage graphs for data transformation models."""
    
    def __init__(self, models: Dict[str, DataModel]):
        self.models = models
        self.model_graph = nx.DiGraph()  # Model-to-model dependencies
        self.column_graph = nx.DiGraph()  # Column-to-column dependencies
        self._build_graphs()
    
    def _build_graphs(self):
        """Build both model-level and column-level lineage graphs."""
        self._build_model_graph()
        self._build_column_graph()
    
    def _build_model_graph(self):
        """Build the model-level lineage graph."""
        # Add all models as nodes
        for model_name, model in self.models.items():
            self.model_graph.add_node(
                model_name,
                model=model,
                layer=getattr(model.model, 'layer', type('Layer', (), {'value': 'unknown'})).value,
                kind=getattr(model.model, 'kind', type('Kind', (), {'value': 'unknown'})).value,
                owner=getattr(model.model, 'owner', 'unknown'),
                domain=getattr(model.model, 'domain', 'unknown'),
                description=getattr(model.model, 'description', '')
            )
        
        # Add dependency edges
        for model_name, model in self.models.items():
            depends_on_tables = getattr(model.source, 'depends_on_tables', [])
            for dependency in depends_on_tables:
                if dependency in self.models:
                    # Check if dependency is in CTEs safely
                    edge_type = 'dependency'  # Default
                    try:
                        if hasattr(model, 'ctes') and model.ctes:
                            ctes_dict = getattr(model.ctes, 'ctes', {})
                            if isinstance(ctes_dict, dict) and dependency in ctes_dict:
                                edge_type = 'cte'
                    except (AttributeError, TypeError):
                        pass  # Use default edge_type
                    
                    self.model_graph.add_edge(
                        dependency, 
                        model_name,
                        edge_type=edge_type,
                        relationship='depends_on'
                    )
    
    def _build_column_graph(self):
        """Build the column-level lineage graph."""
        # Add all columns as nodes
        for model_name, model in self.models.items():
            columns = getattr(model.transformations, 'columns', [])
            for column in columns:
                column_id = f"{model_name}.{column.name}"
                self.column_graph.add_node(
                    column_id,
                    model=model_name,
                    column=column.name,
                    data_type=getattr(column, 'data_type', 'unknown'),
                    expression=getattr(column, 'expression', None),
                    reference_table=getattr(column, 'reference_table', None),
                    description=getattr(column, 'description', '')
                )
        
        # Add column-to-column dependencies
        for model_name, model in self.models.items():
            columns = getattr(model.transformations, 'columns', [])
            for column in columns:
                column_id = f"{model_name}.{column.name}"
                upstream_columns = self._trace_column_dependencies(model_name, column)
                
                for upstream_model, upstream_column, transformation_type in upstream_columns:
                    upstream_column_id = f"{upstream_model}.{upstream_column}"
                    if upstream_column_id in self.column_graph:
                        self.column_graph.add_edge(
                            upstream_column_id,
                            column_id,
                            transformation_type=transformation_type,
                            expression=getattr(column, 'expression', None)
                        )
    
    def _trace_column_dependencies(self, model_name: str, column) -> List[Tuple[str, str, str]]:
        """Trace where a column's data comes from."""
        dependencies = []
        
        # Direct reference case
        if hasattr(column, 'reference_table') and column.reference_table:
            if column.reference_table in self.models:
                # Check if it's a direct column reference or expression
                if hasattr(column, 'expression') and column.expression:
                    # Parse expression to find referenced columns
                    referenced_columns = self._parse_expression_columns(
                        column.expression, 
                        column.reference_table
                    )
                    for ref_column in referenced_columns:
                        dependencies.append((column.reference_table, ref_column, 'expression'))
                else:
                    # Direct column reference
                    dependencies.append((column.reference_table, column.name, 'direct'))
        
        # Look for column references in CTEs
        model = self.models[model_name]
        try:
            if hasattr(model, 'ctes') and model.ctes:
                ctes_dict = getattr(model.ctes, 'ctes', {})
                if isinstance(ctes_dict, dict):
                    for cte_name, cte_model in ctes_dict.items():
                        if cte_model in self.models:
                            # Check if this column might come from a CTE
                            cte_columns = self._get_model_columns(cte_model)
                            if column.name in cte_columns:
                                dependencies.append((cte_model, column.name, 'cte'))
        except (AttributeError, TypeError):
            pass  # Skip CTE processing if there's an issue
        
        # Look for implicit dependencies based on column names
        if not dependencies:
            dependencies.extend(self._find_implicit_column_dependencies(model_name, column))
        
        return dependencies
    
    def _parse_expression_columns(self, expression: str, reference_table: str) -> List[str]:
        """Parse an SQL expression to find referenced column names."""
        if not expression or not reference_table:
            return []
        
        # Get columns from the reference table
        if reference_table not in self.models:
            return []
        
        reference_columns = self._get_model_columns(reference_table)
        found_columns = []
        
        # Simple regex-based parsing (could be enhanced with SQL parser)
        for col_name in reference_columns:
            # Look for column name as whole word (case insensitive)
            pattern = r'\b' + re.escape(col_name) + r'\b'
            if re.search(pattern, expression, re.IGNORECASE):
                found_columns.append(col_name)
        
        return found_columns
    
    def _get_model_columns(self, model_name: str) -> List[str]:
        """Get list of column names for a model."""
        if model_name not in self.models:
            return []
        
        model = self.models[model_name]
        columns = getattr(model.transformations, 'columns', [])
        return [col.name for col in columns]
    
    def _find_implicit_column_dependencies(self, model_name: str, column) -> List[Tuple[str, str, str]]:
        """Find implicit dependencies based on column name matching."""
        dependencies = []
        
        # Look through all upstream models for columns with same name
        upstream_models = list(self.model_graph.predecessors(model_name))
        
        for upstream_model in upstream_models:
            upstream_columns = self._get_model_columns(upstream_model)
            if column.name in upstream_columns:
                dependencies.append((upstream_model, column.name, 'implicit'))
        
        return dependencies
    
    # Enhanced column lineage methods
    def get_column_lineage_detailed(self, model_name: str, column_name: str) -> Dict[str, Any]:
        """Get detailed lineage information for a specific column."""
        column_id = f"{model_name}.{column_name}"
        
        if column_id not in self.column_graph:
            return {
                'column_id': column_id,
                'upstream_columns': [],
                'downstream_columns': [],
                'all_upstream_columns': [],
                'all_downstream_columns': [],
                'transformation_path': []
            }
        
        # Direct upstream columns
        direct_upstream = []
        for pred in self.column_graph.predecessors(column_id):
            edge_data = self.column_graph.edges[pred, column_id]
            upstream_model, upstream_col = pred.split('.', 1)
            direct_upstream.append({
                'model': upstream_model,
                'column': upstream_col,
                'column_id': pred,
                'transformation_type': edge_data.get('transformation_type', 'unknown'),
                'expression': edge_data.get('expression')
            })
        
        # Direct downstream columns
        direct_downstream = []
        for succ in self.column_graph.successors(column_id):
            edge_data = self.column_graph.edges[column_id, succ]
            downstream_model, downstream_col = succ.split('.', 1)
            direct_downstream.append({
                'model': downstream_model,
                'column': downstream_col,
                'column_id': succ,
                'transformation_type': edge_data.get('transformation_type', 'unknown'),
                'expression': edge_data.get('expression')
            })
        
        # All upstream (transitive)
        all_upstream_ids = list(nx.ancestors(self.column_graph, column_id))
        all_upstream = []
        for upstream_id in all_upstream_ids:
            upstream_model, upstream_col = upstream_id.split('.', 1)
            all_upstream.append({
                'model': upstream_model,
                'column': upstream_col,
                'column_id': upstream_id
            })
        
        # All downstream (transitive)
        all_downstream_ids = list(nx.descendants(self.column_graph, column_id))
        all_downstream = []
        for downstream_id in all_downstream_ids:
            downstream_model, downstream_col = downstream_id.split('.', 1)
            all_downstream.append({
                'model': downstream_model,
                'column': downstream_col,
                'column_id': downstream_id
            })
        
        # Build transformation path
        transformation_path = self._build_transformation_path(column_id)
        
        return {
            'column_id': column_id,
            'upstream_columns': direct_upstream,
            'downstream_columns': direct_downstream,
            'all_upstream_columns': all_upstream,
            'all_downstream_columns': all_downstream,
            'transformation_path': transformation_path
        }
    
    def _build_transformation_path(self, column_id: str) -> List[Dict[str, Any]]:
        """Build the transformation path from source to the given column."""
        path = []
        
        # Find all paths from source columns (no predecessors) to this column
        try:
            # Get all source columns (no upstream dependencies)
            source_columns = [node for node in self.column_graph.nodes() 
                            if self.column_graph.in_degree(node) == 0]
            
            # Find paths from each source to our target column
            for source in source_columns:
                try:
                    if nx.has_path(self.column_graph, source, column_id):
                        path_nodes = nx.shortest_path(self.column_graph, source, column_id)
                        path_info = []
                        
                        for i in range(len(path_nodes)):
                            node = path_nodes[i]
                            model, column = node.split('.', 1)
                            step = {
                                'model': model,
                                'column': column,
                                'column_id': node,
                                'step': i + 1
                            }
                            
                            # Add transformation info for non-source steps
                            if i > 0:
                                prev_node = path_nodes[i-1]
                                edge_data = self.column_graph.edges[prev_node, node]
                                step['transformation_type'] = edge_data.get('transformation_type', 'unknown')
                                step['expression'] = edge_data.get('expression')
                            
                            path_info.append(step)
                        
                        path.append({
                            'source_column': source,
                            'path': path_info
                        })
                except nx.NetworkXNoPath:
                    continue
        except Exception:
            # Fallback to simple path if complex path finding fails
            pass
        
        return path
    
    def get_column_impact_analysis(self, model_name: str, column_name: str) -> Dict[str, Any]:
        """Analyze the impact of changes to a specific column."""
        column_id = f"{model_name}.{column_name}"
        
        if column_id not in self.column_graph:
            return {
                'impacted_columns': [],
                'impacted_models': set(),
                'total_impacted_columns': 0,
                'impact_by_model': {}
            }
        
        # Get all downstream columns
        impacted_column_ids = list(nx.descendants(self.column_graph, column_id))
        impacted_columns = []
        impacted_models = set()
        impact_by_model = {}
        
        for col_id in impacted_column_ids:
            model, column = col_id.split('.', 1)
            impacted_models.add(model)
            
            if model not in impact_by_model:
                impact_by_model[model] = []
            impact_by_model[model].append(column)
            
            impacted_columns.append({
                'model': model,
                'column': column,
                'column_id': col_id
            })
        
        return {
            'impacted_columns': impacted_columns,
            'impacted_models': list(impacted_models),
            'total_impacted_columns': len(impacted_columns),
            'impact_by_model': impact_by_model
        }
    
    # Keep existing model-level methods but update to use model_graph
    def get_execution_order(self) -> List[str]:
        """Get topologically sorted execution order."""
        try:
            return list(nx.topological_sort(self.model_graph))
        except nx.NetworkXError as e:
            # Handle circular dependencies
            return list(self.models.keys())
    
    def get_model_lineage(self, model_name: str) -> Dict[str, Any]:
        """Get complete lineage information for a specific model."""
        if model_name not in self.model_graph:
            return {
                'upstream_models': [],
                'downstream_models': [],
                'all_upstream': [],
                'all_downstream': [],
                'lineage_depth': 0
            }
        
        # Direct dependencies (immediate upstream)
        direct_upstream = list(self.model_graph.predecessors(model_name))
        
        # Direct dependents (immediate downstream)
        direct_downstream = list(self.model_graph.successors(model_name))
        
        # All upstream models (transitive dependencies)
        all_upstream = list(nx.ancestors(self.model_graph, model_name))
        
        # All downstream models (transitive dependents)
        all_downstream = list(nx.descendants(self.model_graph, model_name))
        
        return {
            'upstream_models': direct_upstream,
            'downstream_models': direct_downstream,
            'all_upstream': all_upstream,
            'all_downstream': all_downstream,
            'lineage_depth': self._calculate_lineage_depth(model_name)
        }
    
    def get_impact_analysis(self, model_name: str) -> Dict[str, Any]:
        """Analyze the impact of changes to a specific model."""
        if model_name not in self.model_graph:
            return {
                'impacted_models': [],
                'total_impacted_models': 0,
                'impact_by_layer': {},
                'critical_models': []
            }
        
        # Get all downstream models that would be impacted
        impacted_models = list(nx.descendants(self.model_graph, model_name))
        
        # Group by layer
        impact_by_layer = {}
        for impacted_model in impacted_models:
            model = self.models[impacted_model]
            layer = getattr(model.model, 'layer', type('Layer', (), {'value': 'unknown'})).value
            if layer not in impact_by_layer:
                impact_by_layer[layer] = []
            impact_by_layer[layer].append(impacted_model)
        
        # Identify critical models (models with many dependents)
        critical_models = []
        for impacted_model in impacted_models:
            dependents_count = len(list(self.model_graph.successors(impacted_model)))
            if dependents_count >= 3:  # Threshold for "critical"
                critical_models.append(impacted_model)
        
        return {
            'impacted_models': impacted_models,
            'total_impacted_models': len(impacted_models),
            'impact_by_layer': impact_by_layer,
            'critical_models': critical_models
        }
    
    # Legacy method - keeping for backward compatibility
    def get_column_lineage(self, model_name: str, column_name: str) -> Dict[str, Any]:
        """Get lineage information for a specific column (legacy method)."""
        detailed_lineage = self.get_column_lineage_detailed(model_name, column_name)
        
        # Convert to legacy format
        return {
            'source_columns': [
                {
                    'model': col['model'],
                    'column': col['column'],
                    'transformation': col.get('transformation_type', 'unknown')
                }
                for col in detailed_lineage['upstream_columns']
            ],
            'derived_columns': [
                {
                    'model': col['model'],
                    'column': col['column'],
                    'transformation': col.get('transformation_type', 'unknown')
                }
                for col in detailed_lineage['downstream_columns']
            ]
        }
    
    def find_circular_dependencies(self) -> List[List[str]]:
        """Find circular dependencies in the model graph."""
        try:
            cycles = list(nx.simple_cycles(self.model_graph))
            return cycles
        except nx.NetworkXError:
            return []
    
    def find_circular_column_dependencies(self) -> List[List[str]]:
        """Find circular dependencies in the column graph."""
        try:
            cycles = list(nx.simple_cycles(self.column_graph))
            return cycles
        except nx.NetworkXError:
            return []
    
    def get_layer_statistics(self) -> Dict[str, Any]:
        """Get statistics about models by layer."""
        layer_stats = {}
        
        for model_name, model in self.models.items():
            layer = getattr(model.model, 'layer', type('Layer', (), {'value': 'unknown'})).value
            if layer not in layer_stats:
                layer_stats[layer] = {
                    'model_count': 0,
                    'total_columns': 0,
                    'models': []
                }
            
            layer_stats[layer]['model_count'] += 1
            columns = getattr(model.transformations, 'columns', [])
            layer_stats[layer]['total_columns'] += len(columns)
            layer_stats[layer]['models'].append(model_name)
        
        return layer_stats
    
    def get_dependency_statistics(self) -> Dict[str, Any]:
        """Get statistics about model dependencies."""
        stats = {
            'total_models': len(self.models),
            'total_model_dependencies': self.model_graph.number_of_edges(),
            'total_column_dependencies': self.column_graph.number_of_edges(),
            'models_with_no_dependencies': [],
            'models_with_no_dependents': [],
            'most_dependent_models': [],
            'most_depended_upon_models': []
        }
        
        # Find models with no dependencies (source models)
        for model_name in self.models.keys():
            if self.model_graph.in_degree(model_name) == 0:
                stats['models_with_no_dependencies'].append(model_name)
        
        # Find models with no dependents (leaf models)
        for model_name in self.models.keys():
            if self.model_graph.out_degree(model_name) == 0:
                stats['models_with_no_dependents'].append(model_name)
        
        # Find most dependent models (models that depend on many others)
        dependent_counts = [(model, self.model_graph.in_degree(model)) for model in self.models.keys()]
        dependent_counts.sort(key=lambda x: x[1], reverse=True)
        stats['most_dependent_models'] = dependent_counts[:5]
        
        # Find most depended upon models (models that many others depend on)
        depended_counts = [(model, self.model_graph.out_degree(model)) for model in self.models.keys()]
        depended_counts.sort(key=lambda x: x[1], reverse=True)
        stats['most_depended_upon_models'] = depended_counts[:5]
        
        return stats
    
    def _calculate_lineage_depth(self, model_name: str) -> int:
        """Calculate the maximum depth of lineage for a model."""
        if model_name not in self.model_graph:
            return 0
        
        # Find longest path from any source to this model
        try:
            max_depth = 0
            sources = [node for node in self.model_graph.nodes() 
                      if self.model_graph.in_degree(node) == 0]
            
            for source in sources:
                if nx.has_path(self.model_graph, source, model_name):
                    path_length = nx.shortest_path_length(self.model_graph, source, model_name)
                    max_depth = max(max_depth, path_length)
            
            return max_depth
        except:
            return 0
    
    def export_graph_data(self) -> Dict[str, Any]:
        """Export graph data for external visualization tools."""
        # Model nodes and edges
        model_nodes = []
        model_edges = []
        
        for node_id in self.model_graph.nodes():
            node_data = self.model_graph.nodes[node_id]
            model = self.models[node_id]
            columns = getattr(model.transformations, 'columns', [])
            model_nodes.append({
                'id': node_id,
                'label': node_id,
                'layer': node_data.get('layer', 'unknown'),
                'kind': node_data.get('kind', 'unknown'),
                'owner': node_data.get('owner', 'unknown'),
                'domain': node_data.get('domain', 'unknown'),
                'description': node_data.get('description', ''),
                'columns': [col.name for col in columns]
            })
        
        for source, target in self.model_graph.edges():
            edge_data = self.model_graph.edges[source, target]
            model_edges.append({
                'source': source,
                'target': target,
                'type': edge_data.get('edge_type', 'dependency'),
                'relationship': edge_data.get('relationship', 'depends_on')
            })
        
        # Column nodes and edges
        column_nodes = []
        column_edges = []
        
        for node_id in self.column_graph.nodes():
            node_data = self.column_graph.nodes[node_id]
            model_name, column_name = node_id.split('.', 1)
            column_nodes.append({
                'id': node_id,
                'label': f"{model_name}.{column_name}",
                'model': node_data.get('model', model_name),
                'column': node_data.get('column', column_name),
                'data_type': node_data.get('data_type', 'unknown'),
                'expression': node_data.get('expression'),
                'reference_table': node_data.get('reference_table'),
                'description': node_data.get('description', '')
            })
        
        for source, target in self.column_graph.edges():
            edge_data = self.column_graph.edges[source, target]
            column_edges.append({
                'source': source,
                'target': target,
                'transformation_type': edge_data.get('transformation_type', 'unknown'),
                'expression': edge_data.get('expression')
            })
        
        return {
            'model_lineage': {
                'nodes': model_nodes,
                'edges': model_edges
            },
            'column_lineage': {
                'nodes': column_nodes,
                'edges': column_edges
            },
            'statistics': {
                **self.get_dependency_statistics(),
                'layer_statistics': self.get_layer_statistics()
            }
        }
    
    def export_dot_format(self, include_columns: bool = True) -> str:
        """Export graph in DOT format for Graphviz visualization."""
        dot_lines = ['digraph lineage {']
        dot_lines.append('  rankdir=LR;')
        dot_lines.append('  node [shape=record];')
        
        if include_columns:
            # Column-level graph
            for node_id in self.column_graph.nodes():
                node_data = self.column_graph.nodes[node_id]
                model_name = node_data.get('model', 'unknown')
                column_name = node_data.get('column', 'unknown')
                data_type = node_data.get('data_type', '')
                
                label = f"{model_name}\\n{column_name}"
                if data_type:
                    label += f"\\n({data_type})"
                
                dot_lines.append(f'  "{node_id}" [label="{label}"];')
            
            for source, target in self.column_graph.edges():
                edge_data = self.column_graph.edges[source, target]
                transformation_type = edge_data.get('transformation_type', 'unknown')
                dot_lines.append(f'  "{source}" -> "{target}" [label="{transformation_type}"];')
        else:
            # Model-level graph
            for node_id in self.model_graph.nodes():
                node_data = self.model_graph.nodes[node_id]
                layer = node_data.get('layer', 'unknown')
                dot_lines.append(f'  "{node_id}" [label="{node_id}\\n({layer})"];')
            
            for source, target in self.model_graph.edges():
                edge_data = self.model_graph.edges[source, target]
                edge_type = edge_data.get('edge_type', 'dependency')
                dot_lines.append(f'  "{source}" -> "{target}" [label="{edge_type}"];')
        
        dot_lines.append('}')
        return '\n'.join(dot_lines)