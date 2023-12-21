import os
from typing import Any

from laktory.models.basemodel import BaseModel
from laktory._logger import get_logger

logger = get_logger(__name__)


class BaseStack(BaseModel):

    def resolve_vars(self, d: dict, target=None) -> dict[str, Any]:
        _vars = {}
        for k, v in self.variables:
            _vars[f"${{var.{k}}}"] = v

        for k, v in os.environ.items():
            _vars[f"${{var.{k}}}"] = v

        if target == "pulumi":
            _vars["${catalogs."] = "${"
            _vars["${clusters."] = "${"
            _vars["${groups."] = "${"
            _vars["${jobs."] = "${"
            _vars["${notebooks."] = "${"
            _vars["${pipelines."] = "${"
            _vars["${schemas."] = "${"
            _vars["${secret_scopes."] = "${"
            _vars["${sql_queries."] = "${"
            _vars["${tables."] = "${"
            _vars["${users."] = "${"
            _vars["${warehouses."] = "${"
            _vars["${workspace_files."] = "${"
        elif target == "terraform":
            _vars["${catalogs."] = "${databricks_catalog."
            _vars["${clusters."] = "${databricks_cluster."
            _vars["${groups."] = "${databricks_group."
            _vars["${jobs."] = "${databricks_job."
            _vars["${notebooks."] = "${databricks_notebook."
            _vars["${pipelines."] = "${databricks_pipeline."
            _vars["${schemas."] = "${databricks_schema."
            _vars["${secret_scopes."] = "${databricks_secret_scope."
            _vars["${sql_queries."] = "${databricks_sql_query."
            _vars["${tables."] = "${databricks_sql_table."
            _vars["${users."] = "${databricks_user."
            _vars["${warehouses."] = "${databricks_sql_endpoint."
            _vars["${workspace_files."] = "${databricks_workspace_file."

        def search_and_replace(d, old_value, new_val):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = search_and_replace(value, old_value, new_val)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = search_and_replace(item, old_value, new_val)
            elif d == old_value:  # required where d is not a string (bool)
                d = new_val
            elif isinstance(d, str) and old_value in d:
                d = d.replace(old_value, new_val)
            return d

        # Replace variable with their values (except for pulumi output)
        for var_key, var_value in _vars.items():
            d = search_and_replace(d, var_key, var_value)

        return d
