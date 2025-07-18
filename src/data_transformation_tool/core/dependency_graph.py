"""Dependency graph management using NetworkX."""

import networkx as nx
from typing import Dict, List, Set, Any
from .models import DataModel


class DependencyGraph:
    """Manages model dependencies and execution order (lightweight implementation)"""
    
    def __init__(self, models: Dict[str, DataModel]):
        self.models = models
        self.graph = nx.DiGraph()
        self._build_graph()
    
    def _build_graph(self):
        """Build the dependency graph"""
        # Add all models as nodes
        for model_name, model in self.models.items():
            self.graph.add_node(
                model_name, 
                model=model,
                layer=model.model.layer.value,
                kind=model.model.kind.value
            )
        
        # Add dependency edges
        for model_name, model in self.models.items():
            for dep in model.source.depends_on_tables:
                if dep in self.models:
                    self.graph.add_edge(dep, model_name)
    
    def get_execution_order(self) -> List[str]:
        """Get topologically sorted execution order"""
        try:
            return list(nx.topological_sort(self.graph))
        except nx.NetworkXError as e:
            raise ValueError(f"Cannot determine execution order: {str(e)}")
    
    def get_dependencies(self, model_name: str) -> Set[str]:
        """Get all dependencies for a model (transitive)"""
        if model_name not in self.graph:
            return set()
        
        return set(nx.ancestors(self.graph, model_name))
    
    def get_dependents(self, model_name: str) -> Set[str]:
        """Get all models that depend on this model"""
        if model_name not in self.graph:
            return set()
        
        return set(nx.descendants(self.graph, model_name))
    
    def get_direct_dependencies(self, model_name: str) -> List[str]:
        """Get direct dependencies for a model (immediate upstream)"""
        if model_name not in self.graph:
            return []
        
        return list(self.graph.predecessors(model_name))
    
    def get_direct_dependents(self, model_name: str) -> List[str]:
        """Get direct dependents for a model (immediate downstream)"""
        if model_name not in self.graph:
            return []
        
        return list(self.graph.successors(model_name))
    
    def has_circular_dependencies(self) -> bool:
        """Check if there are circular dependencies"""
        return not nx.is_directed_acyclic_graph(self.graph)
    
    def find_circular_dependencies(self) -> List[List[str]]:
        """Find circular dependencies"""
        try:
            return list(nx.simple_cycles(self.graph))
        except nx.NetworkXError:
            return []
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get basic graph statistics"""
        return {
            'total_models': len(self.models),
            'total_dependencies': self.graph.number_of_edges(),
            'has_cycles': self.has_circular_dependencies(),
            'source_models': [node for node in self.graph.nodes() 
                            if self.graph.in_degree(node) == 0],
            'leaf_models': [node for node in self.graph.nodes() 
                          if self.graph.out_degree(node) == 0]
        }
    
    def export_dot(self) -> str:
        """Export graph in DOT format for visualization"""
        dot_lines = ['digraph dependencies {']
        dot_lines.append('  rankdir=LR;')
        dot_lines.append('  node [shape=box];')
        
        # Add nodes with layer coloring
        for node in self.graph.nodes():
            node_data = self.graph.nodes[node]
            layer = node_data.get('layer', 'unknown')
            color = {
                'bronze': 'lightblue',
                'silver': 'lightgreen', 
                'gold': 'lightyellow',
                'cte': 'lightgray'
            }.get(layer, 'white')
            
            dot_lines.append(f'  "{node}" [fillcolor={color}, style=filled, label="{node}\\n({layer})"];')
        
        # Add edges
        for source, target in self.graph.edges():
            dot_lines.append(f'  "{source}" -> "{target}";')
        
        dot_lines.append('}')
        return '\n'.join(dot_lines)