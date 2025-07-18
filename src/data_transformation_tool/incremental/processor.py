from typing import Dict
from ..core.models import DataModel
from ..sql.generator import SQLGenerator

class IncrementalProcessor:
    """Handles incremental processing logic"""
    
    def __init__(self, models: Dict[str, DataModel]):
        self.models = models
    
    def generate_incremental_sql(self, model_name: str, 
                               incremental_strategy: str = 'append') -> str:
        """Generate SQL for incremental processing"""
        if model_name not in self.models:
            raise ValueError(f"Model '{model_name}' not found")
        
        model = self.models[model_name]
        
        if incremental_strategy == 'append':
            return self._generate_append_sql(model)
        elif incremental_strategy == 'merge':
            return self._generate_merge_sql(model)
        elif incremental_strategy == 'delete_insert':
            return self._generate_delete_insert_sql(model)
        else:
            raise ValueError(f"Unsupported incremental strategy: {incremental_strategy}")
    
    def _generate_append_sql(self, model: DataModel) -> str:
        """Generate append-only incremental SQL"""
        base_sql = self._get_base_sql(model)
        table_name = f"{model.model.layer}.{model.model.name}"
        
        # Add watermark filter for new data only
        watermark_condition = self._get_watermark_condition(model)
        
        if watermark_condition:
            base_sql += f"\nAND {watermark_condition}"
        
        return f"""
INSERT INTO {table_name}
{base_sql}
"""
    
    def _generate_merge_sql(self, model: DataModel) -> str:
        """Generate MERGE (upsert) incremental SQL"""
        if not model.grain:
            raise ValueError("MERGE strategy requires grain definition")
        
        base_sql = self._get_base_sql(model)
        table_name = f"{model.model.layer}.{model.model.name}"
        
        # Build MERGE conditions
        merge_conditions = [f"target.{col} = source.{col}" for col in model.grain]
        merge_condition = " AND ".join(merge_conditions)
        
        # Build UPDATE SET clause
        update_columns = [col.name for col in model.transformations.columns if col.name not in model.grain]
        update_sets = [f"target.{col} = source.{col}" for col in update_columns]
        
        return f"""
MERGE INTO {table_name} AS target
USING (
{base_sql}
) AS source
ON {merge_condition}
WHEN MATCHED THEN
  UPDATE SET {', '.join(update_sets)}
WHEN NOT MATCHED THEN
  INSERT ({', '.join([col.name for col in model.transformations.columns])})
  VALUES ({', '.join([f'source.{col.name}' for col in model.transformations.columns])})
"""
    
    def _generate_delete_insert_sql(self, model: DataModel) -> str:
        """Generate delete+insert incremental SQL"""
        base_sql = self._get_base_sql(model)
        table_name = f"{model.model.layer}.{model.model.name}"
        
        # Identify partition to replace
        partition_condition = self._get_partition_condition(model)
        
        return f"""
-- Delete existing data for the partition
DELETE FROM {table_name}
WHERE {partition_condition};

-- Insert new data
INSERT INTO {table_name}
{base_sql}
"""
    
    def _get_base_sql(self, model: DataModel) -> str:
        """Get the base SELECT SQL for the model"""
        sql_generator = SQLGenerator({model.model.name: model})
        return sql_generator._build_select_section(model)
    
    def _get_watermark_condition(self, model: DataModel) -> str:
        """Generate watermark condition for incremental processing"""
        # Look for date/timestamp columns in partitioning
        if model.optimization.partitioned_by:
            date_column = model.optimization.partitioned_by[0]  # Assume first is date
            return f"{date_column} > (SELECT MAX({date_column}) FROM {model.model.layer}.{model.model.name})"
        
        return ""
    
    def _get_partition_condition(self, model: DataModel) -> str:
        """Generate partition condition for delete+insert"""
        if model.optimization.partitioned_by:
            date_column = model.optimization.partitioned_by[0]
            return f"{date_column} = '{{{{ ds }}}}'"  # Airflow template
        
        return "1=1"  # Replace all data if no partitioning