from laktory.sql import py_to_sql
from laktory.workspaceclient import WorkspaceClient


class DatabricksResourcesEngine:

    def __init__(self):
        pass

    @property
    def workspace_client(self):
        return WorkspaceClient()

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
