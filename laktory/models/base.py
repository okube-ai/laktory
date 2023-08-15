import yaml
import json
import jsonref
import time
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict

from laktory import settings


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
    def model_sql_schema(cls):

        def parse(k, field):
            t = field.get("type")
            p1 = field.get("allOf", [])
            p2 = field.get("anyOf", [])

            if t == "array":
                items_field = field['items']
                if len(items_field) == 0:
                    raise ValueError(f"Type for field {k} is undefined")
                else:
                    if "properties" in items_field:
                        _schema = {}
                        for _k, _field in items_field["properties"].items():
                            print(_k, _field)
                            _schema[_k] = parse(_k, _field)
                        return f"array({_schema})"
                    else:
                        return f"array({parse(k, items_field)})"

            elif isinstance(t, str):
                return t.upper()

            elif len(p1) == 1:
                _schema = {}
                for _k, _field in p1[0]["properties"].items():
                    _schema[_k] = parse(_k, _field)
                return _schema

            elif len(p2) > 0:
                _schema = {}
                return parse(k, p2[0])

            else:
                raise ValueError(field)

        json_schema = jsonref.loads(json.dumps(cls.model_json_schema()))

        schema = {}
        for k, f in json_schema["properties"].items():
            if not f.get("to_sql", True):
                continue
            schema[k] = parse(k, f)

        print(schema)

        return str(schema)
