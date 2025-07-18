from typing import Dict, List
from ..core.models import DataModel, ModelKind
from .templates import SQLTemplates
from ..core.dependency_graph import DependencyGraph

class SQLGenerator:
    """Generates SQL code from data models"""
    
    def __init__(self, models: Dict[str, DataModel]):
        self.models = models
        self.templates = SQLTemplates()
        self.dependency_graph = DependencyGraph(models)
        
    def generate_sql(self, model_name: str) -> str:
        """Generate SQL for a specific model"""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
            
        model = self.models[model_name]
        
        if model.model.kind == ModelKind.CTE:
            return self._generate_cte_sql(model)
        elif model.model.kind == ModelKind.VIEW:
            return self._generate_view_sql(model)
        elif model.model.kind == ModelKind.TABLE:
            return self._generate_table_sql(model)
        else:
            raise ValueError(f"Unsupported model kind: {model.model.kind}")
    
    def generate_all_sql(self) -> Dict[str, str]:
        """Generate SQL for all models in dependency order"""
        execution_order = self.dependency_graph.get_execution_order()
        sql_outputs = {}
        
        for model_name in execution_order:
            sql_outputs[model_name] = self.generate_sql(model_name)
            
        return sql_outputs
    
    def _generate_cte_sql(self, model: DataModel) -> str:
        """Generate SQL for CTE models"""
        cte_sql = self._build_cte_section(model)
        select_sql = self._build_select_section(model)
        
        if cte_sql:
            return f"WITH\n{cte_sql}\n{select_sql}"
        else:
            return select_sql
    
    def _generate_view_sql(self, model: DataModel) -> str:
        """Generate CREATE VIEW SQL"""
        schema = model.model.layer
        table_name = model.model.name
        
        cte_sql = self._build_cte_section(model)
        select_sql = self._build_select_section(model)
        
        return self.templates.render_create_view(
            schema=schema,
            table_name=table_name,
            ctes=bool(cte_sql),
            cte_sql=cte_sql,
            select_sql=select_sql
        )
    
    def _generate_table_sql(self, model: DataModel) -> str:
        """Generate CREATE TABLE SQL"""
        schema = model.model.layer
        table_name = model.model.name
        
        cte_sql = self._build_cte_section(model)
        select_sql = self._build_select_section(model)
        
        return self.templates.render_create_table(
            schema=schema,
            table_name=table_name,
            ctes=bool(cte_sql),
            cte_sql=cte_sql,
            select_sql=select_sql,
            partitioned_by=model.optimization.partitioned_by,
            clustered_by=model.optimization.clustered_by
        )
    
    def _build_cte_section(self, model: DataModel) -> str:
        """Build the WITH clause for CTEs"""
        if not model.ctes.ctes:
            return ""
            
        cte_definitions = []
        for cte_name in model.ctes.ctes:
            if cte_name in self.models:
                cte_model = self.models[cte_name]
                cte_sql = self._generate_cte_sql(cte_model)
                cte_definitions.append({
                    'name': cte_name,
                    'sql': cte_sql
                })
        
        return self.templates.render_cte(cte_definitions)
    
    def _build_select_section(self, model: DataModel) -> str:
        """Build the main SELECT clause"""
        columns = self._build_column_expressions(model)
        base_table = self._get_base_table_with_alias(model)
        joins = self._build_join_clauses(model)
        where_conditions = self._build_where_conditions(model)
        
        return self.templates.render_select(
            columns=columns,
            base_table=base_table,
            joins=joins,
            where_conditions=where_conditions,
            group_by=model.aggregations.group_by,
            having=model.aggregations.having
        )
    
    def _build_column_expressions(self, model: DataModel) -> List[Dict]:
        """Build column expressions with proper aliasing"""
        columns = []
        
        for col in model.transformations.columns:
            if col.expression:
                # Use the provided expression
                expression = col.expression
            else:
                # Default to table.column_name
                table_alias = self._get_table_alias(col.reference_table)
                expression = f"{table_alias}.{col.name}"
            
            columns.append({
                'name': col.name,
                'expression': expression
            })
        
        return columns
    
    def _get_base_table_with_alias(self, model: DataModel) -> str:
        """Get the base table with alias"""
        if model.source.base_table:
            base_table = model.source.base_table
        elif model.source.depends_on_tables:
            base_table = model.source.depends_on_tables[0]
        else:
            raise ValueError(f"No base table defined for model {model.model.name}")
        
        alias = self._get_table_alias(base_table)
        return f"{base_table} {alias}"
    
    def _build_join_clauses(self, model: DataModel) -> List[Dict]:
        """Build JOIN clauses from relationships"""
        joins = []
        
        for fk in model.relationships.foreign_keys:
            table_alias = self._get_table_alias(fk.references_table)
            local_table_alias = self._get_table_alias(
                self._find_table_for_column(model, fk.local_column)
            )
            
            condition = f"{local_table_alias}.{fk.local_column} = {table_alias}.{fk.references_column}"
            
            joins.append({
                'type': fk.join_type,
                'table': fk.references_table,
                'alias': table_alias,
                'condition': condition
            })
        
        return joins
    
    def _build_where_conditions(self, model: DataModel) -> List[str]:
        """Build WHERE conditions with proper table aliasing"""
        conditions = []
        
        for filter_cond in model.filters.where_conditions:
            table_alias = self._get_table_alias(filter_cond.reference_table)
            # Replace table references in condition with aliases
            condition = filter_cond.condition.replace(
                f"{filter_cond.reference_table}.", f"{table_alias}."
            )
            conditions.append(condition)
        
        return conditions
    
    def _get_table_alias(self, table_name: str) -> str:
        """Generate consistent table aliases"""
        # Simple strategy: use first letter of each word
        parts = table_name.replace('_', ' ').split()
        return ''.join(part[0].lower() for part in parts if part)
    
    def _find_table_for_column(self, model: DataModel, column_name: str) -> str:
        """Find which table a column belongs to"""
        for col in model.transformations.columns:
            if col.name == column_name:
                return col.reference_table
        
        # Default to base table
        return model.source.base_table or model.source.depends_on_tables[0]