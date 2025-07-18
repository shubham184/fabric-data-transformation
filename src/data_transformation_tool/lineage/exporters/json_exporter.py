"""JSON lineage exporter functionality."""

import json
from typing import Dict, List, Any
from ..graph_builder import LineageGraphBuilder
from ...core.models import DataModel


class JSONLineageExporter:
    """Exports lineage information to JSON format."""
    
    def __init__(self, graph_builder: LineageGraphBuilder):
        self.graph_builder = graph_builder
        self.models = graph_builder.models
    
    def export_full_lineage(self) -> str:
        """Export complete lineage information as JSON."""
        lineage_data = {
            "metadata": self._get_export_metadata(),
            "models": self._export_models(),
            "dependencies": self._export_dependencies(),
            "column_lineage": self._export_column_lineage(),
            "summary": self._get_lineage_summary()
        }
        
        return json.dumps(lineage_data, indent=2, default=str)
    
    def export_model_lineage(self, model_name: str) -> str:
        """Export lineage for a specific model as JSON."""
        model_lineage = self.graph_builder.get_model_lineage(model_name)
        
        # Add detailed information
        detailed_lineage = {
            "model": model_lineage,
            "impact_analysis": self.graph_builder.get_impact_analysis(model_name),
            "root_cause_analysis": self.graph_builder.get_root_cause_analysis(model_name),
            "column_details": self._get_model_column_details(model_name)
        }
        
        return json.dumps(detailed_lineage, indent=2, default=str)
    
    def export_column_lineage(self, model_name: str, column_name: str) -> str:
        """Export lineage for a specific column as JSON."""
        column_lineage = self.graph_builder.get_column_lineage(model_name, column_name)
        
        # Add transformation chain
        transformation_chain = self._build_transformation_chain(model_name, column_name)
        column_lineage["transformation_chain"] = transformation_chain
        
        return json.dumps(column_lineage, indent=2, default=str)
    
    def _get_export_metadata(self) -> Dict[str, Any]:
        """Get metadata about the export."""
        import datetime
        
        return {
            "export_timestamp": datetime.datetime.now().isoformat(),
            "total_models": len(self.models),
            "total_columns": sum(len(model.transformations.columns) for model in self.models.values()),
            "layers": list(set(model.model.layer.value for model in self.models.values())),
            "domains": list(set(model.model.domain for model in self.models.values()))
        }
    
    def _export_models(self) -> List[Dict[str, Any]]:
        """Export model information."""
        models_data = []
        
        for model_name, model in self.models.items():
            model_info = {
                "name": model_name,
                "description": model.model.description,
                "layer": model.model.layer.value,
                "kind": model.model.kind.value,
                "owner": model.model.owner,
                "domain": model.model.domain,
                "tags": model.model.tags,
                "refresh_frequency": model.model.refresh_frequency.value,
                "depends_on": model.source.depends_on_tables,
                "base_table": model.source.base_table,
                "columns": [
                    {
                        "name": col.name,
                        "data_type": col.data_type,
                        "description": col.description,
                        "reference_table": col.reference_table,
                        "expression": col.expression
                    }
                    for col in model.transformations.columns
                ],
                "grain": model.grain,
                "audits": [
                    {
                        "type": audit.type.value,
                        "columns": audit.columns,
                        "values": audit.values
                    }
                    for audit in model.audits.audits
                ]
            }
            models_data.append(model_info)
        
        return models_data
    
    def _export_dependencies(self) -> List[Dict[str, Any]]:
        """Export model dependencies."""
        dependencies = []
        
        for model_name, model in self.models.items():
            for dependency in model.source.depends_on_tables:
                if dependency in self.models:
                    dependencies.append({
                        "source": dependency,
                        "target": model_name,
                        "relationship_type": "model_dependency"
                    })
        
        return dependencies
    
    def _export_column_lineage(self) -> List[Dict[str, Any]]:
        """Export column-level lineage."""
        column_lineage = []
        
        for model_name, model in self.models.items():
            for column in model.transformations.columns:
                if column.reference_table in self.models:
                    source_column = column.expression or column.name
                    
                    lineage_entry = {
                        "source_model": column.reference_table,
                        "source_column": source_column,
                        "target_model": model_name,
                        "target_column": column.name,
                        "transformation": column.expression or "direct_copy",
                        "data_type_source": self._get_column_data_type(column.reference_table, source_column),
                        "data_type_target": column.data_type
                    }
                    column_lineage.append(lineage_entry)
        
        return column_lineage
    
    def _get_lineage_summary(self) -> Dict[str, Any]:
        """Get summary statistics about the lineage."""
        total_models = len(self.models)
        total_dependencies = sum(len(model.source.depends_on_tables) for model in self.models.values())
        total_columns = sum(len(model.transformations.columns) for model in self.models.values())
        
        # Count models by layer
        models_by_layer = {}
        for model in self.models.values():
            layer = model.model.layer.value
            models_by_layer[layer] = models_by_layer.get(layer, 0) + 1
        
        # Count models by domain
        models_by_domain = {}
        for model in self.models.values():
            domain = model.model.domain
            models_by_domain[domain] = models_by_domain.get(domain, 0) + 1
        
        return {
            "total_models": total_models,
            "total_dependencies": total_dependencies,
            "total_columns": total_columns,
            "models_by_layer": models_by_layer,
            "models_by_domain": models_by_domain,
            "complexity_score": self._calculate_complexity_score()
        }
    
    def _get_model_column_details(self, model_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a model."""
        if model_name not in self.models:
            return []
        
        model = self.models[model_name]
        column_details = []
        
        for column in model.transformations.columns:
            try:
                column_lineage = self.graph_builder.get_column_lineage(model_name, column.name)
                column_details.append({
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "description": column.description,
                    "lineage": column_lineage
                })
            except ValueError:
                # Column lineage not available
                column_details.append({
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "description": column.description,
                    "lineage": None
                })
        
        return column_details
    
    def _build_transformation_chain(self, model_name: str, column_name: str) -> List[Dict[str, Any]]:
        """Build the complete transformation chain for a column."""
        chain = []
        current_model = model_name
        current_column = column_name
        visited = set()
        
        while current_model and current_column and (current_model, current_column) not in visited:
            visited.add((current_model, current_column))
            
            if current_model in self.models:
                model = self.models[current_model]
                column_def = next((col for col in model.transformations.columns if col.name == current_column), None)
                
                if column_def:
                    chain.append({
                        "model": current_model,
                        "column": current_column,
                        "data_type": column_def.data_type,
                        "transformation": column_def.expression or "direct_copy",
                        "description": column_def.description
                    })
                    
                    # Move to source
                    if column_def.reference_table in self.models:
                        current_model = column_def.reference_table
                        current_column = column_def.expression or column_def.name
                    else:
                        break
                else:
                    break
            else:
                break
        
        return list(reversed(chain))  # Reverse to show source-to-target flow
    
    def _get_column_data_type(self, model_name: str, column_name: str) -> str:
        """Get data type for a column in a model."""
        if model_name not in self.models:
            return "unknown"
        
        model = self.models[model_name]
        for column in model.transformations.columns:
            if column.name == column_name:
                return column.data_type
        
        return "unknown"
    
    def _calculate_complexity_score(self) -> float:
        """Calculate a complexity score for the lineage."""
        if not self.models:
            return 0.0
        
        total_models = len(self.models)
        total_dependencies = sum(len(model.source.depends_on_tables) for model in self.models.values())
        total_columns = sum(len(model.transformations.columns) for model in self.models.values())
        
        # Simple complexity calculation
        avg_dependencies_per_model = total_dependencies / total_models if total_models > 0 else 0
        avg_columns_per_model = total_columns / total_models if total_models > 0 else 0
        
        complexity = (avg_dependencies_per_model * 0.4) + (avg_columns_per_model * 0.3) + (total_models * 0.3)
        return round(complexity, 2)