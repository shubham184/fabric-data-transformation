from ctypes import Array
from jinja2 import BaseLoader, Environment


def render_sql(sql:str, where : Array[str]) -> str:
        # return f"rendered {cte['cte_stmt']}"
    env=Environment(loader=BaseLoader, extensions=['jinja2_time.TimeExtension'])
    env.trim_blocks=True
    templ_where = '{%for wc in clauses %}{% if loop.first %}\n\nWHERE\n  {% else %}  AND {% endif%}{{wc}} {% endfor %}\n'
    templ_sql = env.from_string(sql)
    where_str = env.from_string(templ_where).render(clauses=where)
    full_sql = templ_sql.render(where=where_str)
    return full_sql