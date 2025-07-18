from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum

class LayerType(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver" 
    GOLD = "gold"
    CTE = "cte"

class ModelKind(str, Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
    CTE = "CTE"

class RefreshFrequency(str, Enum):
    DAILY = "daily"
    HOURLY = "hourly"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL_OUTER = "FULL OUTER"

class RelationshipType(str, Enum):
    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"
    MANY_TO_ONE = "many-to-one"
    MANY_TO_MANY = "many-to-many"

class AuditType(str, Enum):
    NOT_NULL = "NOT_NULL"
    POSITIVE_VALUES = "POSITIVE_VALUES"
    UNIQUE_COMBINATION = "UNIQUE_COMBINATION"
    ACCEPTED_VALUES = "ACCEPTED_VALUES"

class ModelMetadata(BaseModel):
    name: str = Field(..., description="Unique model identifier")
    description: str
    layer: LayerType
    kind: ModelKind
    owner: str
    tags: List[str] = Field(default_factory=list)
    domain: str
    refresh_frequency: RefreshFrequency

class SourceConfig(BaseModel):
    base_table: Optional[str] = None
    depends_on_tables: List[str] = Field(default_factory=list)

class ColumnTransformation(BaseModel):
    name: str
    reference_table: str
    expression: str = ""  # Empty means same name as reference
    description: str
    data_type: str

class TransformationConfig(BaseModel):
    columns: List[ColumnTransformation] = Field(default_factory=list)

class FilterCondition(BaseModel):
    reference_table: str
    condition: str

class FilterConfig(BaseModel):
    where_conditions: List[FilterCondition] = Field(default_factory=list)

class CTEReference(BaseModel):
    name: str

class CTEConfig(BaseModel):
    ctes: List[str] = Field(default_factory=list)  # List of CTE names

class AggregationConfig(BaseModel):
    group_by: List[str] = Field(default_factory=list)
    having: List[str] = Field(default_factory=list)

class AuditRule(BaseModel):
    type: AuditType
    columns: List[str]
    values: Optional[List[str]] = None  # For ACCEPTED_VALUES only

class AuditConfig(BaseModel):
    audits: List[AuditRule] = Field(default_factory=list)

class ForeignKey(BaseModel):
    local_column: str
    references_table: str
    references_column: str
    relationship_type: RelationshipType
    join_type: JoinType

class RelationshipConfig(BaseModel):
    foreign_keys: List[ForeignKey] = Field(default_factory=list)

class IndexConfig(BaseModel):
    columns: List[str]
    type: str

class OptimizationConfig(BaseModel):
    partitioned_by: List[str] = Field(default_factory=list)
    clustered_by: List[str] = Field(default_factory=list)
    indexes: List[IndexConfig] = Field(default_factory=list)

class DataModel(BaseModel):
    model: ModelMetadata
    source: SourceConfig = Field(default_factory=SourceConfig)
    transformations: TransformationConfig = Field(default_factory=TransformationConfig)
    filters: FilterConfig = Field(default_factory=FilterConfig)
    ctes: CTEConfig = Field(default_factory=CTEConfig)
    aggregations: AggregationConfig = Field(default_factory=AggregationConfig)
    audits: AuditConfig = Field(default_factory=AuditConfig)
    grain: List[str] = Field(default_factory=list)
    relationships: RelationshipConfig = Field(default_factory=RelationshipConfig)
    optimization: OptimizationConfig = Field(default_factory=OptimizationConfig)

    @validator('source')
    def validate_cte_dependencies(cls, v, values):
        """Ensure CTEs appear in both depends_on_tables and ctes sections"""
        if 'ctes' in values:
            cte_names = values['ctes'].ctes if hasattr(values['ctes'], 'ctes') else []
            for cte_name in cte_names:
                if cte_name not in v.depends_on_tables:
                    raise ValueError(f"CTE '{cte_name}' must be listed in depends_on_tables")
        return v