import yaml
import json
import jsonref
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict

from laktory.sql import py_to_sql
from laktory.workspaceclient import WorkspaceClient


class BaseModel(_BaseModel):
    model_config = ConfigDict(extra="forbid")

    @property
    def workspace_client(self):
        return WorkspaceClient()

    @classmethod
    def model_validate_yaml(cls, fp):
        data = yaml.safe_load(fp)
        return cls.model_validate(data)

    @classmethod
    def model_validate_json_file(cls, fp):
        data = json.load(fp)
        return cls.model_validate(data)

    # TODO: Migrate to Databricks engine
    @classmethod
    def model_serialized_types(cls):
        def parse(schema_data):
            data_type = schema_data.get("type", None)
            all_of = schema_data.get("allOf", None)
            any_of = schema_data.get("anyOf", None)

            if data_type is None and all_of is not None:
                return parse(all_of[0])

            elif data_type is None and any_of is not None:
                return parse(any_of[0])

            elif data_type == "object":
                properties = schema_data.get("properties", {})
                prop_mapping = {}
                for prop, prop_data in properties.items():
                    prop_mapping[prop] = parse(prop_data)
                return prop_mapping

            elif data_type == "array":
                items = schema_data.get("items", {})
                return [parse(items)]

            else:
                return data_type

        json_schema = jsonref.loads(json.dumps(cls.model_json_schema()))

        types = parse(json_schema)

        return types

    # TODO: Migrate to Databricks engine
    @classmethod
    def model_sql_schema(cls, types: dict = None):
        if types is None:
            types = cls.model_serialized_types()

        schema = (
            "("
            + ", ".join(f"{k} {py_to_sql(v, mode='schema')}" for k, v in types.items())
            + ")"
        )

        if "<>" in schema:
            raise ValueError(
                f"Some types are undefined sql schema can't be defined\n{schema}"
            )

        if "null" in schema:
            raise ValueError(
                f"Some types are undefined sql schema can't be defined\n{schema}"
            )

        return schema
