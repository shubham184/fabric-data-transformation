# YAML Model Structure Documentation

## **File Structure**

```yaml
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: MODEL METADATA
# ═══════════════════════════════════════════════════════════════════════════
model:
  name: [string]              # REQUIRED: Unique model identifier
  description: [string]       # REQUIRED: Business description of the model
  layer: [string]             # REQUIRED: bronze|silver|gold|cte
  kind: [string]              # REQUIRED: TABLE|VIEW|CTE
  owner: [string]             # REQUIRED: Team or person responsible
  tags: [array]               # OPTIONAL: List of tags for categorization
  domain: [string]            # OPTIONAL: product|industry|customer
  refresh_frequency: [string] # OPTIONAL: daily|hourly|weekly|monthly

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: DATA SOURCES & DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════
source:
  base_table: [string]        # OPTIONAL: Primary source table name
  depends_on_tables:          # REQUIRED: List of all dependent tables/models
    - [table_name_1]
    - [table_name_2]
    - [cte_name_1]            # Can include other CTEs

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: COLUMN TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════════════════
transformations:
  columns:                    # REQUIRED: List of output columns
    - name: [string]          # REQUIRED: Output column name
      reference_table: [string] # REQUIRED: Source table/CTE name
      expression: [string]    # OPTIONAL: Column expression (blank = use same name)
      description: [string]   # OPTIONAL: Business description
      data_type: [string]     # OPTIONAL: SQL data type

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: DATA FILTERING
# ═══════════════════════════════════════════════════════════════════════════
filters:
  where_conditions:           # OPTIONAL: List of WHERE clause conditions
    - reference_table: [string] # REQUIRED: Table the condition applies to
      condition: [string]     # REQUIRED: SQL condition (without table prefix)

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: COMMON TABLE EXPRESSIONS
# ═══════════════════════════════════════════════════════════════════════════
ctes:                         # OPTIONAL: List of CTEs to include
  - name: [string]            # REQUIRED: CTE name (references models/ctes/[name].yaml)

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6: AGGREGATION LOGIC
# ═══════════════════════════════════════════════════════════════════════════
aggregations:                # OPTIONAL: Aggregation configuration
  group_by:                   # OPTIONAL: List of GROUP BY columns
    - [column_name_1]
    - [column_name_2]
  having:                     # OPTIONAL: List of HAVING conditions
    - [condition_1]
    - [condition_2]

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7: DATA QUALITY RULES
# ═══════════════════════════════════════════════════════════════════════════
audits:                       # OPTIONAL: List of data quality checks
  - type: [string]            # REQUIRED: NOT_NULL|POSITIVE_VALUES|UNIQUE_COMBINATION
    columns: [array]          # REQUIRED: List of columns to validate

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8: TABLE GRAIN DEFINITION
# ═══════════════════════════════════════════════════════════════════════════
grain:                        # OPTIONAL: List of columns that define uniqueness
  - [column_name_1]
  - [column_name_2]

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 9: TABLE RELATIONSHIPS
# ═══════════════════════════════════════════════════════════════════════════
relationships:                # OPTIONAL: Foreign key relationships
  foreign_keys:               # OPTIONAL: List of foreign key definitions
    - local_column: [string]  # REQUIRED: Column in this model
      references_table: [string] # REQUIRED: Referenced table name
      references_column: [string] # REQUIRED: Referenced column name
      relationship_type: [string] # REQUIRED: one-to-one|one-to-many|many-to-one|many-to-many
      join_type: [string]     # REQUIRED: INNER|LEFT|RIGHT|FULL OUTER

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 10: PERFORMANCE OPTIMIZATION (In Progress)
# ═══════════════════════════════════════════════════════════════════════════
optimization:                # OPTIONAL: Performance hints for database
  partitioned_by: [array]    # OPTIONAL: List of partition columns
  clustered_by: [array]      # OPTIONAL: List of clustering columns
  indexes:                    # OPTIONAL: List of index definitions
    - columns: [array]        # REQUIRED: Columns in the index
      type: [string]          # REQUIRED: Index type (composite, unique, etc.)
```

---

## **Detailed Section Documentation**

### **1. MODEL METADATA**
Defines the basic properties and classification of the model.

```yaml
model:
  name: fct_demand_storage                    # Must be unique across project
  description: "Demand storage fact table"   # Human-readable purpose
  layer: gold                                 # Medallion architecture layer
  kind: TABLE                                 # Determines SQL generation approach
  owner: analytics_team                       # Responsible team/person
  tags: [forecast, demand, daily]            # Searchable categories
  domain: product                             # Business domain classification
  refresh_frequency: daily                    # How often data updates
```

**Field Definitions:**
- **`name`**: Unique identifier used in dependencies and file naming
- **`description`**: Business context for analysts and stakeholders
- **`layer`**: Medallion architecture classification (bronze=raw, silver=clean, gold=aggregated, cte=reusable)
- **`kind`**: Determines SQL generation strategy
  - `TABLE`: Creates physical table (materialized)
  - `VIEW`: Creates database view (virtual)
  - `CTE`: Reusable common table expression
- **`owner`**: Accountability and contact information
- **`tags`**: Searchable metadata for discovery and organization
- **`domain`**: Business domain classification
  - `product`: Product-related data and transformations
  - `industry`: Industry-specific analysis and segmentation
  - `customer`: Customer behavior and analytics
- **`refresh_frequency`**: SLA and scheduling hints

### **2. DATA SOURCES & DEPENDENCIES**
Defines what data this model depends on and uses for lineage tracking.

```yaml
source:
  base_table: fct_demand_storage             # Primary source (optional)
  depends_on_tables:                         # All dependencies (required)
    - fct_demand_storage                     # Bronze layer tables
    - FCT_FORECASTCYCLE                      # Source tables
    - FCT_FORECASTGROUP                      # Dimension tables
    - active_forecast_cycles                 # CTEs (also listed in ctes section)
    - forecast_summary                       # CTEs (also listed in ctes section)
```

**Field Definitions:**
- **`base_table`**: Primary source table (used when most columns come from one table)
- **`depends_on_tables`**: Complete list of all dependencies including:
  - Source tables from bronze/silver layers
  - Dimension and lookup tables
  - Other CTEs (these must also be listed in the `ctes` section)
  - Used for lineage tracking, dependency graph building, and validation

**Validation Rules:**
- All `reference_table` values in transformations must exist in `depends_on_tables`
- All `reference_table` values in filters must exist in `depends_on_tables`
- All CTE names in `ctes` section must also appear in `depends_on_tables`
- Circular dependencies are not allowed

### **3. COLUMN TRANSFORMATIONS**
Defines the output columns and how they're calculated from source data.

```yaml
transformations:
  columns:
    # Simple 1:1 mapping
    - name: forecast_cycle_id
      reference_table: FCT_FORECASTCYCLE      # WHERE: source table
      expression: ""                          # WHAT: blank = same column name
      description: "Unique forecast cycle identifier"
      data_type: INTEGER
      
    # Column renaming
    - name: cycle_name
      reference_table: FCT_FORECASTCYCLE
      expression: "cycle_description"         # WHAT: different column name
      description: "Forecast cycle description"
      data_type: VARCHAR(255)
      
    # Calculated column
    - name: total_demand_quantity
      reference_table: demand_summary_cte
      expression: "SUM(base_quantity)"       # WHAT: aggregation function
      description: "Total demand quantity for this cycle"
      data_type: DECIMAL(18,2)
```

**Field Definitions:**
- **`name`**: Output column name in the generated model
- **`reference_table`**: Source table or CTE (must be in `depends_on_tables`)
- **`expression`**: 
  - Blank (`""`): Use column with same name from `reference_table`
  - Column name (`"order_amount"`): Use specific column from `reference_table`
  - SQL expression (`"SUM(order_amount)"`): Apply function/calculation
- **`description`**: Business meaning for documentation
- **`data_type`**: SQL data type for schema validation

**Pattern Examples:**
```yaml
# Pattern 1: Direct column copy
expression: ""                    # Takes forecast_cycle_id from reference_table

# Pattern 2: Column renaming  
expression: "cycle_code"          # Takes cycle_code, outputs as forecast_cycle_id

# Pattern 3: Simple calculation
expression: "base_quantity * 1.1" # Applies 10% buffer

# Pattern 4: Aggregation
expression: "SUM(demand_quantity)" # Requires aggregations section

# Pattern 5: Complex logic
expression: "CASE WHEN forecast_type = 'confirmed' THEN 1 ELSE 0 END"
```

### **4. DATA FILTERING**
Defines WHERE conditions applied before any aggregation.

```yaml
filters:
  where_conditions:
    - reference_table: FCT_FORECASTCYCLE
      condition: "is_active = 1"              # Active forecast cycles only
      
    - reference_table: fct_demand_storage
      condition: "forecast_date >= '2024-01-01'"  # Recent forecasts only
      
    - reference_table: FCT_FORECASTGROUP
      condition: "forecast_type IN ('confirmed', 'planned')"  # Specific forecast types
```

**Field Definitions:**
- **`reference_table`**: Table the condition applies to (must be in `depends_on_tables`)
- **`condition`**: SQL WHERE condition (without table prefix - tool adds this)

**Generated SQL:**
```sql
WHERE FCT_FORECASTCYCLE.is_active = 1
  AND fct_demand_storage.forecast_date >= '2024-01-01'
  AND FCT_FORECASTGROUP.forecast_type IN ('confirmed', 'planned')
```

### **5. COMMON TABLE EXPRESSIONS**
References to other CTE models that this model depends on.

```yaml
ctes:
  - name: active_forecast_cycles        # References models/ctes/active_forecast_cycles.yaml
  - name: demand_summary               # References models/ctes/demand_summary.yaml
```

**Field Definitions:**
- **`name`**: Name of CTE (must match a `models/ctes/[name].yaml` file)
- **Important**: All CTE names listed here must also appear in `depends_on_tables` section

**Generated SQL:**
```sql
WITH active_forecast_cycles AS (
  -- SQL from models/ctes/active_forecast_cycles.yaml
),
demand_summary AS (
  -- SQL from models/ctes/demand_summary.yaml
)
SELECT ...
```

**CTE Chaining:**
CTEs can reference other CTEs, creating complex business logic chains:
```
base_forecast_cte → filtered_forecast_cte → calculated_demand_cte → final_model
```

### **6. AGGREGATION LOGIC**
Defines GROUP BY and HAVING clauses for aggregated models.

```yaml
aggregations:
  group_by:                          # Columns that define aggregation groups
    - forecast_cycle_id              # References OUTPUT column from transformations
    - forecast_group_id              # References OUTPUT column from transformations
    - forecast_month                 # References OUTPUT column from transformations
    
  having:                            # Conditions applied after aggregation
    - "total_demand > 1000"               # References OUTPUT column alias
    - "item_count >= 5"                   # References OUTPUT column alias  
    - "SUM(base_quantity) > 1000"         # OR references same expression as in transformations
```

**Field Definitions:**
- **`group_by`**: List of output column names from transformations section
  - Must match the `name` field of non-aggregated columns in transformations
  - These become the GROUP BY columns in generated SQL
- **`having`**: List of conditions for HAVING clause
  - Can reference output column `name` values from transformations
  - Can reference the same expressions used in transformations
  - Applied after aggregation (vs WHERE which is before)

**Validation Rules:**
- All columns in `group_by` must exist as `name` values in transformations (non-aggregated)
- All aggregated columns in transformations require `group_by` to be specified  
- `having` conditions should reference either:
  - Output column names from transformations, OR
  - The same expressions used in transformations

**Generated SQL:**
```sql
SELECT 
    forecast_cycle_id, 
    forecast_group_id, 
    SUM(base_quantity) as total_demand,
    COUNT(forecast_item_id) as item_count
FROM forecast_data
GROUP BY forecast_cycle_id, forecast_group_id  -- References SELECT columns
HAVING total_demand > 1000                     -- References SELECT column alias
```

### **7. DATA QUALITY RULES**
Automatic validation checks that run against the generated data.

```yaml
audits:
  - type: NOT_NULL               # No empty values allowed
    columns: [forecast_cycle_id, forecast_date]
    
  - type: POSITIVE_VALUES        # Must be greater than zero
    columns: [base_quantity, calculated_quantity]
    
  - type: UNIQUE_COMBINATION     # No duplicate combinations
    columns: [forecast_cycle_id, forecast_item_id]
    
  - type: ACCEPTED_VALUES        # Must be in allowed list
    columns: [forecast_status]
    values: ['active', 'inactive', 'planned']
```

**Available Audit Types:**
- **`NOT_NULL`**: Ensures columns never contain NULL values
- **`POSITIVE_VALUES`**: Ensures numeric columns are greater than zero
- **`UNIQUE_COMBINATION`**: Ensures column combination is unique across rows
- **`ACCEPTED_VALUES`**: Ensures columns only contain specified values (requires `values` field)

**Field Definitions:**
- **`type`**: Type of validation check
- **`columns`**: List of columns to validate (must exist in transformations)
- **`values`**: List of accepted values (only for `ACCEPTED_VALUES` type)

### **8. TABLE GRAIN DEFINITION**
Defines what makes each row unique in the table - the "grain" of the data.

```yaml
grain:
  - forecast_cycle_id           # Each row represents one forecast cycle
  - forecast_item_id           # For a specific forecast item
```

**Purpose:**
- **Documentation**: Clearly states what each row represents
- **Validation**: Can be used to detect unexpected duplicates
- **Optimization**: Hints for partitioning and indexing strategies

**Examples by Model Type:**
```yaml
# Forecast cycle dimension - one row per cycle
grain: [forecast_cycle_id]

# Daily demand fact - one row per item per date
grain: [forecast_item_id, forecast_date]

# Forecast line items - one row per item per cycle
grain: [forecast_cycle_id, forecast_item_id]
```

### **9. TABLE RELATIONSHIPS**
Defines how this model relates to other tables through foreign keys.

```yaml
relationships:
  foreign_keys:
    - local_column: forecast_cycle_id     # Column in this model
      references_table: FCT_FORECASTCYCLE # Referenced table
      references_column: cycle_id         # Referenced column
      relationship_type: many-to-one      # Relationship cardinality
      join_type: INNER                    # How to join tables
```

**Field Definitions:**
- **`local_column`**: Column in this model (must exist in transformations)
- **`references_table`**: Table being referenced (must exist in project)
- **`references_column`**: Column in the referenced table
- **`relationship_type`**: Cardinality of the relationship
  - `one-to-one`: Each row matches exactly one row
  - `one-to-many`: One parent row has multiple child rows
  - `many-to-one`: Multiple rows reference one parent row
  - `many-to-many`: Rows can relate to multiple rows on both sides
- **`join_type`**: SQL join strategy
  - `INNER`: Only keep rows that have matches (required data)
  - `LEFT`: Keep all rows from left table (optional relationship)
  - `RIGHT`: Keep all rows from right table
  - `FULL OUTER`: Keep all rows from both tables

**Generated SQL:**
```sql
FROM fct_demand_storage fds
INNER JOIN FCT_FORECASTCYCLE fc ON fds.forecast_cycle_id = fc.cycle_id
LEFT JOIN FCT_FORECASTGROUP fg ON fds.forecast_group_id = fg.group_id
```

### **10. PERFORMANCE OPTIMIZATION**
Database-specific hints for improving query performance.

```yaml
optimization:
  partitioned_by: [forecast_date]              # Partition table by date
  clustered_by: [forecast_cycle_id]            # Cluster data by cycle
  indexes:
    - columns: [forecast_cycle_id, forecast_date] # Composite index
      type: composite
    - columns: [forecast_item_id]                # Unique index
      type: unique
```

**Field Definitions:**
- **`partitioned_by`**: Columns to partition table by (usually date columns)
- **`clustered_by`**: Columns to physically sort data by
- **`indexes`**: List of index definitions
  - `columns`: Columns included in the index
  - `type`: Index type (`composite`, `unique`, `btree`, etc.)

**Benefits:**
- **Partitioning**: Faster queries on date ranges, easier maintenance
- **Clustering**: Better compression, faster scans on clustered columns
- **Indexes**: Faster lookups and joins

---

## **Model Kind Specifications**

### **TABLE Models**
Creates physical tables with materialized data.

**Required Sections:** `model`, `source`, `transformations`
**Optional Sections:** All others
**Use Cases:** Fact tables, aggregated metrics, final business views
**Performance:** Fast reads, slower writes, uses storage

### **VIEW Models**  
Creates database views that query underlying tables in real-time.

**Required Sections:** `model`, `source`, `transformations`
**Optional Sections:** All others except `optimization`
**Use Cases:** Raw data access, real-time views, simple transformations
**Performance:** No storage used, performance depends on underlying tables

### **CTE Models**
Reusable common table expressions for complex business logic.

**Required Sections:** `model`, `source`, `transformations`
**Optional Sections:** All others
**Use Cases:** Shared business logic, complex calculations, data preparation
**Special Feature:** Can reference other CTEs creating chains of logic

---

## **Validation Rules**

### **Cross-Section Validation**
1. All `reference_table` values must exist in `depends_on_tables`
2. All `local_column` values in relationships must exist in transformations
3. All columns in `grain` must exist in transformations
4. All columns in `audits` must exist in transformations
5. **AGGREGATIONS VALIDATION:**
   - All columns in `group_by` must exist as `name` values in transformations (non-aggregated)
   - All aggregated expressions in transformations require `group_by` to be specified
   - `having` conditions should reference either output column names OR same expressions as transformations
   - If model has any aggregated columns, it MUST have aggregations section

### **Model Kind Validation**
1. **CTE models**: Cannot have `optimization` section
2. **TABLE models**: Can have all sections
3. **VIEW models**: Cannot have certain optimization features

### **Business Logic Validation**
1. CTE references must form a directed acyclic graph (no circular dependencies)
2. Foreign key relationships must be consistent with transformations
3. Aggregations and non-aggregations cannot be mixed without proper GROUP BY

---

## **Example Usage Patterns**

### **Simple Bronze Layer View**
```yaml
model:
  kind: VIEW
source:
  base_table: raw_forecast_data
transformations:
  columns:
    - name: forecast_id
      reference_table: raw_forecast_data
      expression: ""
filters:
  where_conditions:
    - reference_table: raw_forecast_data
      condition: "is_deleted = 0"
```

### **Complex Gold Layer Aggregation**
```yaml
model:
  kind: TABLE
source:
  depends_on_tables: [fct_demand_storage, FCT_FORECASTCYCLE, FCT_FORECASTGROUP]
transformations:
  columns:
    - name: forecast_group_id       # Non-aggregated (for GROUP BY)
      reference_table: FCT_FORECASTGROUP
      expression: "group_id"
    - name: total_demand           # Aggregated column with alias
      reference_table: fct_demand_storage
      expression: "SUM(base_quantity)"
    - name: item_count             # Aggregated column with alias  
      reference_table: fct_demand_storage
      expression: "COUNT(item_id)"
aggregations:
  group_by: [forecast_group_id]    # References OUTPUT column name
  having: 
    - "total_demand > 10000"       # References OUTPUT column alias
    - "item_count >= 5"            # References OUTPUT column alias
```

### **Reusable CTE**
```yaml
model:
  kind: CTE
source:
  base_table: FCT_FORECASTCYCLE
transformations:
  columns:
    - name: cycle_id
      reference_table: FCT_FORECASTCYCLE
      expression: ""
    - name: demand_category
      reference_table: FCT_FORECASTCYCLE
      expression: "CASE WHEN total_quantity > 1000 THEN 'high' ELSE 'standard' END"
filters:
  where_conditions:
    - reference_table: FCT_FORECASTCYCLE
      condition: "status = 'active'"
```

This structure provides complete flexibility while maintaining consistency across all model types.