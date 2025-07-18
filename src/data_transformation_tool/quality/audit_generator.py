from typing import Dict, List
from ..core.models import DataModel, AuditRule, AuditType

class AuditSQLGenerator:
    """Generates data quality audit SQL"""
    
    def generate_audit_sql(self, model: DataModel) -> Dict[str, str]:
        """Generate SQL for all audit rules"""
        audit_queries = {}
        
        for i, audit in enumerate(model.audits.audits):
            audit_name = f"{model.model.name}_audit_{i+1}_{audit.type.value.lower()}"
            
            if audit.type == AuditType.NOT_NULL:
                sql = self._generate_not_null_audit(model, audit)
            elif audit.type == AuditType.POSITIVE_VALUES:
                sql = self._generate_positive_values_audit(model, audit)
            elif audit.type == AuditType.UNIQUE_COMBINATION:
                sql = self._generate_unique_combination_audit(model, audit)
            elif audit.type == AuditType.ACCEPTED_VALUES:
                sql = self._generate_accepted_values_audit(model, audit)
            else:
                continue
            
            audit_queries[audit_name] = sql
        
        return audit_queries
    
    def _generate_not_null_audit(self, model: DataModel, audit: AuditRule) -> str:
        """Generate NOT NULL audit SQL"""
        table_name = f"{model.model.layer}.{model.model.name}"
        conditions = [f"{col} IS NULL" for col in audit.columns]
        
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  'NOT_NULL' as audit_type,
  '{', '.join(audit.columns)}' as columns_checked,
  COUNT(*) as failed_rows
FROM {table_name}
WHERE {' OR '.join(conditions)}
HAVING COUNT(*) > 0
"""

    def _generate_positive_values_audit(self, model: DataModel, audit: AuditRule) -> str:
        """Generate POSITIVE_VALUES audit SQL"""
        table_name = f"{model.model.layer}.{model.model.name}"
        conditions = [f"{col} <= 0" for col in audit.columns]
        
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  'POSITIVE_VALUES' as audit_type,
  '{', '.join(audit.columns)}' as columns_checked,
  COUNT(*) as failed_rows
FROM {table_name}
WHERE {' OR '.join(conditions)}
HAVING COUNT(*) > 0
"""

    def _generate_unique_combination_audit(self, model: DataModel, audit: AuditRule) -> str:
        """Generate UNIQUE_COMBINATION audit SQL"""
        table_name = f"{model.model.layer}.{model.model.name}"
        columns = ', '.join(audit.columns)
        
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  'UNIQUE_COMBINATION' as audit_type,
  '{columns}' as columns_checked,
  COUNT(*) - COUNT(DISTINCT {columns}) as failed_rows
FROM {table_name}
HAVING COUNT(*) - COUNT(DISTINCT {columns}) > 0
"""

    def _generate_accepted_values_audit(self, model: DataModel, audit: AuditRule) -> str:
        """Generate ACCEPTED_VALUES audit SQL"""
        table_name = f"{model.model.layer}.{model.model.name}"
        
        if len(audit.columns) != 1:
            raise ValueError("ACCEPTED_VALUES audit supports only one column")
        
        column = audit.columns[0]
        accepted_values = "', '".join(audit.values)
        
        return f"""
SELECT 
  '{model.model.name}' as model_name,
  'ACCEPTED_VALUES' as audit_type,
  '{column}' as columns_checked,
  COUNT(*) as failed_rows
FROM {table_name}
WHERE {column} NOT IN ('{accepted_values}')
HAVING COUNT(*) > 0
"""