import yaml
import json
import jsonref
import time
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict

from laktory import settings
from laktory.sql import value_to_statement


class BaseModel(_BaseModel):
    model_config = ConfigDict(extra="forbid")

    @property
    def workspace_client(self):
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )

    def execute_statement_and_wait(self, statement, warehouse_id=None, catalog_name=None):

        from databricks.sdk.service.sql import StatementState

        w = self.workspace_client

        # Settings
        if warehouse_id is None:
            warehouse_id = settings.databricks_warehouse_id

        if catalog_name is None:
            catalog_name = getattr(self, "catalog_name", None)

        r = w.statement_execution.execute_statement(
            statement=statement,
            catalog=catalog_name,
            warehouse_id=warehouse_id,
        )
        statement_id = r.statement_id
        state = r.status.state

        while state in [StatementState.PENDING, StatementState.RUNNING]:
            r = w.statement_execution.get_statement(statement_id)
            time.sleep(1)
            state = r.status.state

        if state != StatementState.SUCCEEDED:
            # TODO: Create specific error
            print(r.status.error.message)
            raise Exception(r.status.error)

        return r

    @classmethod
    def model_validate_yaml(cls, fp):
        data = yaml.safe_load(fp)
        return cls.model_validate(data)

    @classmethod
    def model_validate_json_file(cls, fp):
        data = json.load(fp)
        return cls.model_validate(data)

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

    @classmethod
    def model_sql_schema(cls, types: dict = None, overwrites: dict = None):
        if types is None:
            types = cls.model_serialized_types()

        schema = "(" + ", ".join(f"{k} {value_to_statement(v, mode='schema')}" for k, v in types.items()) + ")"

        if "<>" in schema:
            raise ValueError("Some types are undefined sql schema can't be defined")

        if "null" in schema:
            raise ValueError("Some types are undefined sql schema can't be defined")

        return schema
