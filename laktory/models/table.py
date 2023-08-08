from typing import Literal

from pydantic import computed_field

from laktory import settings
from laktory.models.base import BaseModel
from laktory.models.column import Column
from laktory.models.sources.tablesource import TableSource
from laktory.models.sources.eventsource import EventSource


class Table(BaseModel):
    name: str
    columns: list[Column] = []
    primary_key: str = None
    comment: str = None
    catalog_name: str = None
    database_name: str = None

    # Lakehouse
    event_source: EventSource = None
    table_source: TableSource = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    pipeline_name: str = None
    # joins
    # expectations

    @computed_field
    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.database_name:
            if _id == "":
                _id = self.database_name
            else:
                _id += f".{self.database_name}"

        return _id

    @computed_field
    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @computed_field
    @property
    def schema_name(self) -> str:
        return self.database_name

    @classmethod
    def meta_table(cls):
        columns = []
        for c in cls.model_fields.keys():
            columns += [
                Column(name=c, type="string")
            ]

        return Table(
            name="tables",
            database_name="laktory",
            columns=columns,
        )

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def exists(self):
        return self.name in [c.name for c in self.workspace_client.tables.list(
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
        )]

    def create(self, if_not_exists: bool = True, or_replace: bool = False):

        w = self.workspace_client

        if or_replace:
            if_not_exists = False

        statement = f"CREATE  "
        if or_replace:
            statement += "OR REPLACE "
        statement += "TABLE "
        if if_not_exists:
            statement += "IF NOT EXISTS "

        statement += f"{self.schema_name}.{self.name}"

        w.statement_execution.execute_statement(
            statement=statement,
            catalog=self.catalog_name,
            warehouse_id="faeced4d6221b239",
        )
        #
        # exists = self.exists()
        #
        # if if_not_exists and exists:
        #     return w.tables.get(self.name)
        #
        # return w.catalogs.create(
        #     name=self.name,
        #     comment=self.comment,
        # )

    def delete(self, force: bool = False):
        self.workspace_client.tables.delete(self.full_name, force=force)


    #
    # def sql_create(
    #         self,
    #         or_replace: bool = True,
    #         create_catalog: bool = True,
    #         create_database: bool = True,
    # ):
    #     from databricks.sdk import WorkspaceClient
    #
    #     catalog_name = self.catalog_name
    #     database_name = self.database_name
    #     if catalog_name is None:
    #         raise ValueError(f"Catalog name for table {self.name} is not defined.")
    #     if database_name is None:
    #         raise ValueError(f"Database name for table {self.name} is not defined.")
    #
    #     w = WorkspaceClient(
    #         host=settings.databricks_host,
    #         token=settings.databricks_token,
    #     )
    #
    #     # catalog_exists = catalog_name in [c.name for w.catalogs.list()]
    #     # if not catalog_exists:
    #     #     w.catalogs.create(catalog_name)
    #     # else:
    #
    #
    #     # if create_catalog:
    #     # catalog = w.catalogs.get(catalog_name)
    #
    #     # # List schemas
    #     # schemas = w.schemas.list(catalog_name=catalog.name)
    #     # for s in schemas:
    #     #     print(s.name)
    #     #
    #     # # Create laktory schema
    #     # schema_exists = database_name in [s.name for s in schemas]
    #     # if schema_exists:
    #     #     w.schemas.delete(f"{catalog_name}.{database_name}")
    #     # w.schemas.create(
    #     #     database_name,
    #     #     catalog_name=catalog_name,
    #     #     force=True,
    #     # )
    #     #
    #     # # --------------------------------------------------------------------------- #
    #     # # Create Tables Table                                                         #
    #     # # --------------------------------------------------------------------------- #
    #     #
    #     # table_name = "tables"
    #     # w.statement_execution.execute_statement(
    #     #     statement=f"CREATE OR REPLACE TABLE {database_name}.{table_name}",
    #     #     catalog=catalog_name,
    #     #     warehouse_id="faeced4d6221b239",
    #     # )


if __name__ == "__main__":
    table = Table(
        name="f1549",
        columns=[
            {
                "name": "airspeed",
                "type": "double",
            },
            {
                "name": "altitude",
                "type": "double",
            },
        ],
    )

    print(table)
