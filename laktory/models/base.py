import yaml
import json
import jsonref
from typing import Any
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict

from laktory.workspaceclient import WorkspaceClient


class BaseModel(_BaseModel):
    model_config = ConfigDict(extra="forbid")
    # _vars: dict[str, Any] = {}

    def __init__(self, _vars=None, **kwargs):
        super().__init__(**kwargs)
        # Setting a private field does not seem to be propagated to all child
        # classes. Using the init instead.
        if _vars is None:
            _vars = {}
        self._vars = _vars

    @property
    def workspace_client(self):
        return WorkspaceClient()

    @property
    def vars(self) -> dict[str, Any]:
        return self._vars

    @vars.setter
    def vars(self, value):
        self._vars = value

    @classmethod
    def model_validate_yaml(cls, fp):
        data = yaml.safe_load(fp)
        return cls.model_validate(data)

    @classmethod
    def model_validate_json_file(cls, fp):
        data = json.load(fp)
        return cls.model_validate(data)

    #
    # def model_pulumi_dump(self):
    #     return self.model_dump()

    @property
    def pulumi_excludes(self) -> list[str]:
        return []

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {}

    def inject_vars(self, d) -> dict:
        def search_and_replace(d, old_value, new_val):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = search_and_replace(value, old_value, new_val)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = search_and_replace(item, old_value, new_val)
            elif d == old_value:
                d = new_val
            elif isinstance(d, str) and old_value in d:
                d = d.replace(old_value, new_val)
            return d

        for var_key, var_value in self.vars.items():
            d = search_and_replace(d, f"${{var.{var_key}}}", var_value)
        return d

    def model_pulumi_dump(self, *args, **kwargs):
        kwargs["exclude"] = self.pulumi_excludes
        d = super().model_dump(*args, **kwargs)
        for k, v in self.pulumi_renames.items():
            d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d

    # TODO: Migrate to Databricks engine
    # @classmethod
    # def model_serialized_types(cls):
    #     def parse(schema_data):
    #         data_type = schema_data.get("type", None)
    #         all_of = schema_data.get("allOf", None)
    #         any_of = schema_data.get("anyOf", None)
    #
    #         if data_type is None and all_of is not None:
    #             return parse(all_of[0])
    #
    #         elif data_type is None and any_of is not None:
    #             return parse(any_of[0])
    #
    #         elif data_type == "object":
    #             properties = schema_data.get("properties", {})
    #             prop_mapping = {}
    #             for prop, prop_data in properties.items():
    #                 prop_mapping[prop] = parse(prop_data)
    #             return prop_mapping
    #
    #         elif data_type == "array":
    #             items = schema_data.get("items", {})
    #             return [parse(items)]
    #
    #         else:
    #             return data_type
    #
    #     json_schema = jsonref.loads(json.dumps(cls.model_json_schema()))
    #
    #     types = parse(json_schema)
    #
    #     return types

    # TODO: Migrate to Databricks engine
    # @classmethod
    # def model_sql_schema(cls, types: dict = None):
    #     if types is None:
    #         types = cls.model_serialized_types()
    #
    #     schema = (
    #         "("
    #         + ", ".join(f"{k} {py_to_sql(v, mode='schema')}" for k, v in types.items())
    #         + ")"
    #     )
    #
    #     if "<>" in schema:
    #         raise ValueError(
    #             f"Some types are undefined sql schema can't be defined\n{schema}"
    #         )
    #
    #     if "null" in schema:
    #         raise ValueError(
    #             f"Some types are undefined sql schema can't be defined\n{schema}"
    #         )
    #
    #     return schema
