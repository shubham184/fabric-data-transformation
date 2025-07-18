from typing import List, Dict
from jinja2 import Environment, BaseLoader, Template

class SQLTemplates:
    """Manages SQL generation templates"""
    
    # CTE template
    CTE_TEMPLATE = """
{%- for cte in ctes %}
{{ cte.name }} AS (
{{ cte.sql | indent(2, True) }}
){{ "," if not loop.last else "" }}
{%- endfor %}
"""

    # SELECT template
    SELECT_TEMPLATE = """
SELECT
{%- for column in columns %}
  {{ column.expression }} AS {{ column.name }}{{ "," if not loop.last else "" }}
{%- endfor %}
FROM {{ base_table }}
{%- for join in joins %}
{{ join.type }} JOIN {{ join.table }} {{ join.alias }} ON {{ join.condition }}
{%- endfor %}
{%- if where_conditions %}
WHERE
{%- for condition in where_conditions %}
  {{ condition }}{{ " AND" if not loop.last else "" }}
{%- endfor %}
{%- endif %}
{%- if group_by %}
GROUP BY {{ group_by | join(", ") }}
{%- endif %}
{%- if having %}
HAVING {{ having | join(" AND ") }}
{%- endif %}
"""

    # CREATE TABLE template  
    CREATE_TABLE_TEMPLATE = """
CREATE TABLE {{ schema }}.{{ table_name }}
{%- if partitioned_by %}
USING DELTA
PARTITIONED BY ({{ partitioned_by | join(", ") }})
{%- endif %}
{%- if clustered_by %}
CLUSTERED BY ({{ clustered_by | join(", ") }})
{%- endif %}
AS
{%- if ctes %}
WITH
{{ cte_sql }}
{%- endif %}
{{ select_sql }}
"""

    # CREATE VIEW template
    CREATE_VIEW_TEMPLATE = """
CREATE VIEW {{ schema }}.{{ table_name }} AS
{%- if ctes %}
WITH
{{ cte_sql }}
{%- endif %}
{{ select_sql }}
"""

    def __init__(self):
        self.env = Environment(loader=BaseLoader())
        
    def render_cte(self, ctes: List[Dict]) -> str:
        template = self.env.from_string(self.CTE_TEMPLATE)
        return template.render(ctes=ctes)
    
    def render_select(self, **kwargs) -> str:
        template = self.env.from_string(self.SELECT_TEMPLATE)
        return template.render(**kwargs)
    
    def render_create_table(self, **kwargs) -> str:
        template = self.env.from_string(self.CREATE_TABLE_TEMPLATE)
        return template.render(**kwargs)
    
    def render_create_view(self, **kwargs) -> str:
        template = self.env.from_string(self.CREATE_VIEW_TEMPLATE)
        return template.render(**kwargs)