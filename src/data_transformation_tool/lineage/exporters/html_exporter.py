"""HTML lineage exporter for interactive visualization."""

import json
from typing import Dict, List
from ..graph_builder import LineageGraphBuilder
from ...core.models import DataModel


class HTMLLineageExporter:
    """Exports lineage information to interactive HTML format."""
    
    def __init__(self, graph_builder: LineageGraphBuilder):
        self.graph_builder = graph_builder
        self.models = graph_builder.models
    
    def export_interactive_lineage(self, title: str = "Data Lineage") -> str:
        """Export interactive HTML lineage visualization."""
        # Prepare data for visualization
        nodes_data = self._prepare_nodes_data()
        edges_data = self._prepare_edges_data()
        
        html_template = self._get_html_template()
        
        # Replace placeholders with actual data
        html_content = html_template.replace(
            '{{TITLE}}', title
        ).replace(
            '{{NODES_DATA}}', json.dumps(nodes_data)
        ).replace(
            '{{EDGES_DATA}}', json.dumps(edges_data)
        ).replace(
            '{{MODELS_INFO}}', json.dumps(self._get_models_info())
        )
        
        return html_content
    
    def export_model_detail_page(self, model_name: str) -> str:
        """Export detailed HTML page for a specific model."""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        model = self.models[model_name]
        model_lineage = self.graph_builder.get_model_lineage(model_name)
        impact_analysis = self.graph_builder.get_impact_analysis(model_name)
        
        template = self._get_model_detail_template()
        
        # Replace placeholders
        html_content = template.replace(
            '{{MODEL_NAME}}', model_name
        ).replace(
            '{{MODEL_INFO}}', json.dumps(self._get_single_model_info(model))
        ).replace(
            '{{MODEL_LINEAGE}}', json.dumps(model_lineage)
        ).replace(
            '{{IMPACT_ANALYSIS}}', json.dumps(impact_analysis)
        )
        
        return html_content
    
    def _prepare_nodes_data(self) -> List[Dict]:
        """Prepare nodes data for visualization."""
        nodes = []
        
        for model_name, model in self.models.items():
            node = {
                'id': model_name,
                'label': model_name,
                'title': self._create_node_tooltip(model),
                'group': model.model.layer.value,
                'shape': self._get_node_shape(model.model.kind.value),
                'color': self._get_node_color(model.model.layer.value),
                'font': {'size': 12},
                'physics': True
            }
            nodes.append(node)
        
        return nodes
    
    def _prepare_edges_data(self) -> List[Dict]:
        """Prepare edges data for visualization."""
        edges = []
        edge_id = 0
        
        for model_name, model in self.models.items():
            for dependency in model.source.depends_on_tables:
                if dependency in self.models:
                    edge = {
                        'id': edge_id,
                        'from': dependency,
                        'to': model_name,
                        'arrows': 'to',
                        'color': {'color': '#848484'},
                        'width': 2,
                        'title': f"{dependency} → {model_name}"
                    }
                    
                    # Different style for CTE dependencies
                    if dependency in model.ctes.ctes:
                        edge['dashes'] = True
                        edge['color'] = {'color': '#2E86C1'}
                        edge['title'] += ' (CTE)'
                    
                    edges.append(edge)
                    edge_id += 1
        
        return edges
    
    def _get_models_info(self) -> Dict:
        """Get comprehensive models information."""
        models_info = {}
        
        for model_name, model in self.models.items():
            models_info[model_name] = self._get_single_model_info(model)
        
        return models_info
    
    def _get_single_model_info(self, model: DataModel) -> Dict:
        """Get information for a single model."""
        return {
            'name': model.model.name,
            'description': model.model.description,
            'layer': model.model.layer.value,
            'kind': model.model.kind.value,
            'owner': model.model.owner,
            'domain': model.model.domain,
            'tags': model.model.tags,
            'refresh_frequency': model.model.refresh_frequency.value,
            'depends_on': model.source.depends_on_tables,
            'base_table': model.source.base_table,
            'columns': [
                {
                    'name': col.name,
                    'data_type': col.data_type,
                    'description': col.description,
                    'reference_table': col.reference_table,
                    'expression': col.expression
                }
                for col in model.transformations.columns
            ],
            'audits': [
                {
                    'type': audit.type.value,
                    'columns': audit.columns,
                    'values': audit.values
                }
                for audit in model.audits.audits
            ],
            'grain': model.grain
        }
    
    def _create_node_tooltip(self, model: DataModel) -> str:
        """Create tooltip text for a node."""
        tooltip_parts = [
            f"<b>{model.model.name}</b>",
            f"Layer: {model.model.layer.value}",
            f"Kind: {model.model.kind.value}",
            f"Owner: {model.model.owner}",
            f"Domain: {model.model.domain}"
        ]
        
        if model.model.description:
            tooltip_parts.append(f"Description: {model.model.description}")
        
        if model.transformations.columns:
            tooltip_parts.append(f"Columns: {len(model.transformations.columns)}")
        
        return "<br>".join(tooltip_parts)
    
    def _get_node_shape(self, kind: str) -> str:
        """Get node shape based on model kind."""
        shapes = {
            'TABLE': 'box',
            'VIEW': 'ellipse',
            'CTE': 'diamond'
        }
        return shapes.get(kind, 'box')
    
    def _get_node_color(self, layer: str) -> str:
        """Get node color based on layer."""
        colors = {
            'bronze': '#87CEEB',
            'silver': '#98FB98',
            'gold': '#FFD700',
            'cte': '#D3D3D3'
        }
        return colors.get(layer, '#FFFFFF')
    
    def _get_html_template(self) -> str:
        """Get the main HTML template."""
        return '''<!DOCTYPE html>
<html>
<head>
    <title>{{TITLE}}</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style type="text/css">
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
        #network { width: 100%; height: 600px; border: 1px solid lightgray; }
        #info-panel { margin-top: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .model-info { background-color: #f9f9f9; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .column-list { max-height: 200px; overflow-y: auto; }
        .control-panel { margin-bottom: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px; }
        button { margin: 5px; padding: 8px 15px; cursor: pointer; }
        select { margin: 5px; padding: 5px; }
        .legend { margin: 10px 0; }
        .legend-item { display: inline-block; margin: 5px 10px; }
        .legend-color { display: inline-block; width: 20px; height: 20px; margin-right: 5px; vertical-align: middle; }
    </style>
</head>
<body>
    <h1>{{TITLE}}</h1>
    
    <div class="control-panel">
        <button onclick="fitNetwork()">Fit to Screen</button>
        <button onclick="showAllNodes()">Show All</button>
        <select id="layerFilter" onchange="filterByLayer()">
            <option value="">All Layers</option>
            <option value="bronze">Bronze</option>
            <option value="silver">Silver</option>
            <option value="gold">Gold</option>
            <option value="cte">CTE</option>
        </select>
        <select id="modelSelect" onchange="focusOnModel()">
            <option value="">Select Model</option>
        </select>
    </div>
    
    <div class="legend">
        <div class="legend-item">
            <span class="legend-color" style="background-color: #87CEEB;"></span>Bronze Layer
        </div>
        <div class="legend-item">
            <span class="legend-color" style="background-color: #98FB98;"></span>Silver Layer
        </div>
        <div class="legend-item">
            <span class="legend-color" style="background-color: #FFD700;"></span>Gold Layer
        </div>
        <div class="legend-item">
            <span class="legend-color" style="background-color: #D3D3D3;"></span>CTE
        </div>
    </div>
    
    <div id="network"></div>
    
    <div id="info-panel">
        <h3>Model Information</h3>
        <div id="model-details">Click on a node to see model details</div>
    </div>

    <script type="text/javascript">
        var nodes = new vis.DataSet({{NODES_DATA}});
        var edges = new vis.DataSet({{EDGES_DATA}});
        var modelsInfo = {{MODELS_INFO}};
        
        var container = document.getElementById('network');
        var data = { nodes: nodes, edges: edges };
        var options = {
            layout: {
                hierarchical: {
                    direction: 'LR',
                    sortMethod: 'directed',
                    levelSeparation: 150,
                    nodeSpacing: 100
                }
            },
            physics: {
                enabled: true,
                hierarchicalRepulsion: {
                    centralGravity: 0.0,
                    springLength: 100,
                    springConstant: 0.01,
                    nodeDistance: 120,
                    damping: 0.09
                },
                maxVelocity: 50,
                solver: 'hierarchicalRepulsion',
                timestep: 0.35,
                stabilization: {iterations: 150}
            },
            interaction: {
                dragNodes: true,
                dragView: true,
                zoomView: true
            }
        };
        
        var network = new vis.Network(container, data, options);
        
        // Populate model select dropdown
        var modelSelect = document.getElementById('modelSelect');
        Object.keys(modelsInfo).sort().forEach(function(modelName) {
            var option = document.createElement('option');
            option.value = modelName;
            option.textContent = modelName;
            modelSelect.appendChild(option);
        });
        
        // Event listener for node clicks
        network.on("click", function (params) {
            if (params.nodes.length > 0) {
                var modelName = params.nodes[0];
                showModelDetails(modelName);
            }
        });
        
        function showModelDetails(modelName) {
            var model = modelsInfo[modelName];
            if (!model) return;
            
            var html = '<div class="model-info">';
            html += '<h4>' + model.name + '</h4>';
            html += '<p><strong>Description:</strong> ' + (model.description || 'N/A') + '</p>';
            html += '<p><strong>Layer:</strong> ' + model.layer + '</p>';
            html += '<p><strong>Kind:</strong> ' + model.kind + '</p>';
            html += '<p><strong>Owner:</strong> ' + model.owner + '</p>';
            html += '<p><strong>Domain:</strong> ' + model.domain + '</p>';
            html += '<p><strong>Refresh Frequency:</strong> ' + model.refresh_frequency + '</p>';
            
            if (model.depends_on && model.depends_on.length > 0) {
                html += '<p><strong>Dependencies:</strong> ' + model.depends_on.join(', ') + '</p>';
            }
            
            if (model.columns && model.columns.length > 0) {
                html += '<p><strong>Columns (' + model.columns.length + '):</strong></p>';
                html += '<div class="column-list"><ul>';
                model.columns.forEach(function(col) {
                    html += '<li><strong>' + col.name + '</strong> (' + col.data_type + ')';
                    if (col.description) {
                        html += ' - ' + col.description;
                    }
                    html += '</li>';
                });
                html += '</ul></div>';
            }
            
            html += '</div>';
            
            document.getElementById('model-details').innerHTML = html;
        }
        
        function fitNetwork() {
            network.fit();
        }
        
        function showAllNodes() {
            nodes.clear();
            nodes.add({{NODES_DATA}});
            edges.clear();
            edges.add({{EDGES_DATA}});
        }
        
        function filterByLayer() {
            var selectedLayer = document.getElementById('layerFilter').value;
            
            if (selectedLayer === '') {
                showAllNodes();
                return;
            }
            
            var filteredNodes = {{NODES_DATA}}.filter(function(node) {
                return node.group === selectedLayer;
            });
            
            var nodeIds = filteredNodes.map(function(node) { return node.id; });
            var filteredEdges = {{EDGES_DATA}}.filter(function(edge) {
                return nodeIds.includes(edge.from) && nodeIds.includes(edge.to);
            });
            
            nodes.clear();
            nodes.add(filteredNodes);
            edges.clear();
            edges.add(filteredEdges);
        }
        
        function focusOnModel() {
            var selectedModel = document.getElementById('modelSelect').value;
            if (selectedModel === '') return;
            
            network.focus(selectedModel, {
                scale: 1.0,
                animation: {
                    duration: 1000,
                    easingFunction: 'easeInOutQuad'
                }
            });
            
            showModelDetails(selectedModel);
        }
    </script>
</body>
</html>'''
    
    def _get_model_detail_template(self) -> str:
        """Get the model detail page template."""
        return '''<!DOCTYPE html>
<html>
<head>
    <title>Model Details: {{MODEL_NAME}}</title>
    <style type="text/css">
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; line-height: 1.6; }
        .container { max-width: 1200px; margin: 0 auto; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .section h3 { margin-top: 0; color: #333; }
        .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .info-item { background-color: #f9f9f9; padding: 10px; border-radius: 3px; }
        .column-table { width: 100%; border-collapse: collapse; margin: 10px 0; }
        .column-table th, .column-table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .column-table th { background-color: #f2f2f2; }
        .back-link { margin-bottom: 20px; }
        .badge { display: inline-block; padding: 3px 8px; background-color: #007bff; color: white; border-radius: 3px; font-size: 0.8em; margin: 2px; }
        .layer-bronze { background-color: #87CEEB; }
        .layer-silver { background-color: #98FB98; }
        .layer-gold { background-color: #FFD700; color: #333; }
        .layer-cte { background-color: #D3D3D3; color: #333; }
    </style>
</head>
<body>
    <div class="container">
        <div class="back-link">
            <a href="javascript:history.back()">← Back to Lineage</a>
        </div>
        
        <h1>Model Details: {{MODEL_NAME}}</h1>
        
        <div id="model-info-section" class="section">
            <h3>Model Information</h3>
            <div id="model-info-content"></div>
        </div>
        
        <div id="lineage-section" class="section">
            <h3>Lineage Information</h3>
            <div id="lineage-content"></div>
        </div>
        
        <div id="impact-section" class="section">
            <h3>Impact Analysis</h3>
            <div id="impact-content"></div>
        </div>
        
        <div id="columns-section" class="section">
            <h3>Column Details</h3>
            <div id="columns-content"></div>
        </div>
    </div>

    <script type="text/javascript">
        var modelInfo = {{MODEL_INFO}};
        var modelLineage = {{MODEL_LINEAGE}};
        var impactAnalysis = {{IMPACT_ANALYSIS}};
        
        function loadModelInfo() {
            var html = '<div class="info-grid">';
            
            html += '<div class="info-item">';
            html += '<h4>Basic Information</h4>';
            html += '<p><strong>Name:</strong> ' + modelInfo.name + '</p>';
            html += '<p><strong>Description:</strong> ' + (modelInfo.description || 'N/A') + '</p>';
            html += '<p><strong>Layer:</strong> <span class="badge layer-' + modelInfo.layer + '">' + modelInfo.layer + '</span></p>';
            html += '<p><strong>Kind:</strong> ' + modelInfo.kind + '</p>';
            html += '<p><strong>Owner:</strong> ' + modelInfo.owner + '</p>';
            html += '<p><strong>Domain:</strong> ' + modelInfo.domain + '</p>';
            html += '<p><strong>Refresh Frequency:</strong> ' + modelInfo.refresh_frequency + '</p>';
            html += '</div>';
            
            html += '<div class="info-item">';
            html += '<h4>Dependencies</h4>';
            if (modelInfo.depends_on && modelInfo.depends_on.length > 0) {
                html += '<ul>';
                modelInfo.depends_on.forEach(function(dep) {
                    html += '<li>' + dep + '</li>';
                });
                html += '</ul>';
            } else {
                html += '<p>No dependencies</p>';
            }
            html += '</div>';
            
            if (modelInfo.tags && modelInfo.tags.length > 0) {
                html += '<div class="info-item">';
                html += '<h4>Tags</h4>';
                modelInfo.tags.forEach(function(tag) {
                    html += '<span class="badge">' + tag + '</span>';
                });
                html += '</div>';
            }
            
            html += '</div>';
            document.getElementById('model-info-content').innerHTML = html;
        }
        
        function loadLineageInfo() {
            var html = '<div class="info-grid">';
            
            html += '<div class="info-item">';
            html += '<h4>Upstream Models</h4>';
            if (modelLineage.upstream_models && modelLineage.upstream_models.length > 0) {
                html += '<ul>';
                modelLineage.upstream_models.forEach(function(model) {
                    html += '<li>' + model + '</li>';
                });
                html += '</ul>';
            } else {
                html += '<p>No upstream models</p>';
            }
            html += '</div>';
            
            html += '<div class="info-item">';
            html += '<h4>Downstream Models</h4>';
            if (modelLineage.downstream_models && modelLineage.downstream_models.length > 0) {
                html += '<ul>';
                modelLineage.downstream_models.forEach(function(model) {
                    html += '<li>' + model + '</li>';
                });
                html += '</ul>';
            } else {
                html += '<p>No downstream models</p>';
            }
            html += '</div>';
            
            html += '</div>';
            document.getElementById('lineage-content').innerHTML = html;
        }
        
        function loadImpactAnalysis() {
            var html = '<p><strong>Total Impacted Models:</strong> ' + impactAnalysis.total_impacted_models + '</p>';
            
            if (impactAnalysis.impact_by_layer && Object.keys(impactAnalysis.impact_by_layer).length > 0) {
                html += '<h4>Impact by Layer:</h4>';
                html += '<ul>';
                Object.keys(impactAnalysis.impact_by_layer).forEach(function(layer) {
                    var models = impactAnalysis.impact_by_layer[layer];
                    html += '<li><strong>' + layer + ':</strong> ' + models.join(', ') + '</li>';
                });
                html += '</ul>';
            }
            
            document.getElementById('impact-content').innerHTML = html;
        }
        
        function loadColumnsInfo() {
            if (!modelInfo.columns || modelInfo.columns.length === 0) {
                document.getElementById('columns-content').innerHTML = '<p>No columns defined</p>';
                return;
            }
            
            var html = '<table class="column-table">';
            html += '<thead><tr><th>Name</th><th>Data Type</th><th>Description</th><th>Reference Table</th><th>Expression</th></tr></thead>';
            html += '<tbody>';
            
            modelInfo.columns.forEach(function(col) {
                html += '<tr>';
                html += '<td>' + col.name + '</td>';
                html += '<td>' + col.data_type + '</td>';
                html += '<td>' + (col.description || '') + '</td>';
                html += '<td>' + (col.reference_table || '') + '</td>';
                html += '<td>' + (col.expression || 'direct copy') + '</td>';
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            document.getElementById('columns-content').innerHTML = html;
        }
        
        // Load all sections
        loadModelInfo();
        loadLineageInfo();
        loadImpactAnalysis();
        loadColumnsInfo();
    </script>
</body>
</html>'''