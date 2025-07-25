import json
from jinja2 import Environment, FileSystemLoader

import merge_utils as mu
from dag_util import DagEnabledElementInterface
import sql_utils

@DagEnabledElementInterface.register
class B2S_Definition:
    def __init__(self, name):
        self.target = name
        self.predecessors = []
        self.base_table = "name"
        self.columns = []
        self.where = []
        self.ctes = []

    def get_id(self):
        """DagEnabledElementInterface method"""
        return self.target
    
    def get_predecessors(self):
        """DagEnabledElementInterface method"""
        return self.predecessors
    
    def add_filedata(self, filedata: dict):
        mu.update_simple_props(filedata, self.__dict__, ["predecessors", "base_table"])
        if "columns" in filedata:
            for col in filedata["columns"]:
                mu.merge_data_to_list(
                    col, self.columns, "ColumnName", ["Expression", "Comment"]
                )
        if "where" in filedata:
            for whereclause in filedata["where"]:
                mu.merge_data_to_list(whereclause, self.where, "id", ["clause"])
        if ("ctes") in filedata:
            for cte in filedata["ctes"]:
                new_cte = mu.merge_data_to_list(
                    cte, self.ctes, "CTE", ["cte_join", "cte_stmt"]
                )
                if "where" not in new_cte:
                    new_cte["where"] = []
                if "where" in cte:
                    for where_clause in cte["where"]:
                        mu.merge_data_to_list(
                            where_clause, new_cte["where"], "id", ["clause"]
                        )

    def save_to_json(self, filename: str):
        with open(filename, "w") as f:
            json.dump(self.__dict__, f, indent=2)

    def get_where_clauses(self):
        result = [w["clause"] for w in self.where] + [
            cte["cte_join"] for cte in self.ctes
        ]
        return result

    def render_cte(self, cte):
        rslt = sql_utils.render_sql(
            cte["cte_stmt"], where=[w["clause"] for w in cte["where"]]
        )
        return rslt

    def get_ctes(self):
        rslt = [
            {"CTE": cte["CTE"], "cte_stmt": self.render_cte(cte)} for cte in self.ctes
        ]
        return rslt

    def save_to_sql(self, filename: str):
        env = Environment(
            loader=FileSystemLoader("buildtools/"),
            extensions=["jinja2_time.TimeExtension"],
        )
        env.trim_blocks = True
        template = env.get_template("b2s.sql")
        # self.ctes = []
        with open(filename, "w") as f:
            f.write(template.render(b2s=self))
