"""DOT (Graphviz) lineage exporter functionality."""

from typing import Dict, List, Set
from ..graph_builder import LineageGraphBuilder
from ...core.models import DataModel


class DOTLineageExporter:
    """Exports lineage information to DOT format for Graphviz visualization."""
    
    def __init__(self, graph_builder: LineageGraphBuilder):
        self.graph_builder = graph_builder
        self.models = graph_builder.models
        
        # Color scheme for different layers
        self.layer_colors = {
            'bronze': '#87CEEB',      # Sky blue
            'silver': '#98FB98',      # Pale green
            'gold': '#FFD700',        # Gold
            'cte': '#D3D3D3'          # Light gray
        }
        
        # Shape scheme for different model kinds
        self.kind_shapes = {
            'TABLE': 'box',
            'VIEW': 'ellipse',
            'CTE': 'diamond'
        }
    
    def export_model_lineage(self, include_columns: bool = False) -> str:
        """Export model lineage as DOT format."""
        dot_lines = [
            'digraph model_lineage {',
            '  rankdir=LR;',
            '  node [fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=8];',
            ''
        ]
        
        # Add model nodes
        dot_lines.extend(self._generate_model_nodes())
        dot_lines.append('')
        
        # Add model dependencies
        dot_lines.extend(self._generate_model_edges())
        dot_lines.append('')
        
        # Add column nodes and edges if requested
        if include_columns:
            dot_lines.extend(self._generate_column_nodes())
            dot_lines.append('')
            dot_lines.extend(self._generate_column_edges())
            dot_lines.append('')
        
        # Add clusters by layer
        dot_lines.extend(self._generate_layer_clusters())
        
        dot_lines.append('}')
        
        return '\\n'.join(dot_lines)
    
    def export_model_focus(self, model_name: str, depth: int = 2) -> str:
        """Export focused view of a specific model and its neighbors."""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        # Get models within specified depth
        focus_models = self._get_models_within_depth(model_name, depth)
        
        dot_lines = [
            f'digraph model_focus_{model_name.replace(".", "_")} {{',
            '  rankdir=LR;',
            '  node [fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=8];',
            ''
        ]
        
        # Add focused model nodes
        dot_lines.extend(self._generate_model_nodes(focus_models))
        dot_lines.append('')
        
        # Add focused edges
        dot_lines.extend(self._generate_model_edges(focus_models))
        dot_lines.append('')
        
        # Highlight the focus model
        dot_lines.append(f'  "{model_name}" [style=filled, fillcolor=yellow, penwidth=3];')
        
        dot_lines.append('}')
        
        return '\\n'.join(dot_lines)
    
    def export_impact_analysis(self, model_name: str) -> str:
        """Export impact analysis visualization."""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        impact_analysis = self.graph_builder.get_impact_analysis(model_name)
        impacted_models = set(impact_analysis['impacted_models'])
        impacted_models.add(model_name)
        
        dot_lines = [
            f'digraph impact_analysis_{model_name.replace(".", "_")} {{',
            '  rankdir=LR;',
            '  node [fontname="Arial", fontsize=10];',
            '  edge [fontname="Arial", fontsize=8];',
            '  label="Impact Analysis: Changes to ' + model_name + '";',
            '  labelloc=t;',
            ''
        ]
        
        # Add model nodes
        dot_lines.extend(self._generate_model_nodes(impacted_models))
        dot_lines.append('')
        
        # Add edges
        dot_lines.extend(self._generate_model_edges(impacted_models))
        dot_lines.append('')
        
        # Highlight source model
        dot_lines.append(f'  "{model_name}" [style=filled, fillcolor=red, label="{model_name}\\n(SOURCE)"];')
        
        # Highlight impacted models
        for impacted_model in impact_analysis['impacted_models']:
            dot_lines.append(f'  "{impacted_model}" [style=filled, fillcolor=orange];')
        
        dot_lines.append('}')
        
        return '\\n'.join(dot_lines)
    
    def _generate_model_nodes(self, filter_models: Set[str] = None) -> List[str]:
        """Generate DOT nodes for models."""
        nodes = []
        
        for model_name, model in self.models.items():
            if filter_models and model_name not in filter_models:
                continue
                
            layer = model.model.layer.value
            kind = model.model.kind.value
            
            # Get node attributes
            color = self.layer_colors.get(layer, '#FFFFFF')
            shape = self.kind_shapes.get(kind, 'box')
            
            # Create label with model info
            label_parts = [model_name]
            if model.model.description:
                # Truncate long descriptions
                desc = model.model.description[:30] + '...' if len(model.model.description) > 30 else model.model.description
                label_parts.append(desc)
            
            label = '\\n'.join(label_parts)
            
            node_line = f'  "{model_name}" [label="{label}", shape={shape}, style=filled, fillcolor="{color}"];'
            nodes.append(node_line)
        
        return nodes
    
    def _generate_model_edges(self, filter_models: Set[str] = None) -> List[str]:
        """Generate DOT edges for model dependencies."""
        edges = []
        
        for model_name, model in self.models.items():
            if filter_models and model_name not in filter_models:
                continue
                
            for dependency in model.source.depends_on_tables:
                if dependency in self.models:
                    if filter_models and dependency not in filter_models:
                        continue
                        
                    # Determine edge style based on dependency type
                    if dependency in model.ctes.ctes:
                        edge_style = 'style=dashed, color=blue'
                        edge_label = 'CTE'
                    else:
                        edge_style = 'color=black'
                        edge_label = ''
                    
                    if edge_label:
                        edge_line = f'  "{dependency}" -> "{model_name}" [{edge_style}, label="{edge_label}"];'
                    else:
                        edge_line = f'  "{dependency}" -> "{model_name}" [{edge_style}];'
                    
                    edges.append(edge_line)
        
        return edges
    
    def _generate_column_nodes(self) -> List[str]:
        """Generate DOT nodes for columns."""
        nodes = []
        
        for model_name, model in self.models.items():
            for column in model.transformations.columns:
                column_id = f"{model_name}.{column.name}"
                
                # Create column label
                label_parts = [column.name, column.data_type]
                if column.description:
                    desc = column.description[:20] + '...' if len(column.description) > 20 else column.description
                    label_parts.append(desc)
                
                label = '\\n'.join(label_parts)
                
                node_line = f'  "{column_id}" [label="{label}", shape=oval, style=filled, fillcolor=lightblue, fontsize=8];'
                nodes.append(node_line)
        
        return nodes
    
    def _generate_column_edges(self) -> List[str]:
        """Generate DOT edges for column lineage."""
        edges = []
        
        for model_name, model in self.models.items():
            for column in model.transformations.columns:
                target_column_id = f"{model_name}.{column.name}"
                
                if column.reference_table in self.models:
                    source_column_name = column.expression or column.name
                    source_column_id = f"{column.reference_table}.{source_column_name}"
                    
                    # Check if source column exists
                    if self._column_exists(column.reference_table, source_column_name):
                        edge_style = 'color=gray, fontsize=6'
                        transformation = column.expression or 'copy'
                        
                        if transformation != 'copy' and len(transformation) > 20:
                            transformation = transformation[:20] + '...'
                        
                        edge_line = f'  "{source_column_id}" -> "{target_column_id}" [{edge_style}, label="{transformation}"];'
                        edges.append(edge_line)
        
        return edges
    
    def _generate_layer_clusters(self) -> List[str]:
        """Generate DOT clusters for grouping models by layer."""
        clusters = []
        
        # Group models by layer
        models_by_layer = {}
        for model_name, model in self.models.items():
            layer = model.model.layer.value
            if layer not in models_by_layer:
                models_by_layer[layer] = []
            models_by_layer[layer].append(model_name)
        
        # Create clusters
        for layer, model_names in models_by_layer.items():
            if len(model_names) > 1:  # Only create cluster if multiple models
                cluster_color = self.layer_colors.get(layer, '#FFFFFF')
                
                clusters.append(f'  subgraph cluster_{layer} {{')
                clusters.append(f'    label="{layer.upper()} LAYER";')
                clusters.append(f'    style=filled;')
                clusters.append(f'    fillcolor="{cluster_color}";')
                clusters.append(f'    alpha=0.3;')
                
                for model_name in model_names:
                    clusters.append(f'    "{model_name}";')
                
                clusters.append('  }')
                clusters.append('')
        
        return clusters
    
    def _get_models_within_depth(self, model_name: str, depth: int) -> Set[str]:
        """Get all models within specified depth from the given model."""
        models_in_scope = {model_name}
        current_level = {model_name}
        
        for _ in range(depth):
            next_level = set()
            
            for current_model in current_level:
                if current_model in self.models:
                    model = self.models[current_model]
                    
                    # Add dependencies (upstream)
                    for dep in model.source.depends_on_tables:
                        if dep in self.models:
                            next_level.add(dep)
                    
                    # Add dependents (downstream)
                    for other_model_name, other_model in self.models.items():
                        if current_model in other_model.source.depends_on_tables:
                            next_level.add(other_model_name)
            
            models_in_scope.update(next_level)
            current_level = next_level
        
        return models_in_scope
    
    def _column_exists(self, model_name: str, column_name: str) -> bool:
        """Check if a column exists in a model."""
        if model_name not in self.models:
            return False
        
        model = self.models[model_name]
        column_names = {col.name for col in model.transformations.columns}
        return column_name in column_names