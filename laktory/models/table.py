import time
from typing import Literal
from typing import Any

from pydantic import computed_field
from pydantic import model_validator

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

    # Data
    data: list[list[Any]] = None

    # Lakehouse
    event_source: EventSource = None
    table_source: TableSource = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    pipeline_name: str = None
    # joins
    # expectations

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode="after")
    def assign_table_to_columns(self) -> Any:
        for c in self.columns:
            c.table_name = self.name
            c.catalog_name = self.catalog_name
            c.database_name = self.database_name

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

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

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def column_names(self):
        return [c.name for c in self.columns]

    @property
    def df(self):
        import pandas as pd
        return pd.DataFrame(data=self.data, columns=self.column_names)

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

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

    def create(self,
               if_not_exists: bool = True,
               or_replace: bool = False,
               insert_data: bool = False,
               warehouse_id: str = None,
               ):

        from databricks.sdk.service.sql import StatementState

        if len(self.columns) == 0:
            raise ValueError()

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

        statement += "("
        for c in self.columns:
            statement += f"{c.name} {c.type},"
        statement = statement[:-1]
        statement += ")"

        if warehouse_id is None:
            warehouse_id = settings.databricks_warehouse_id
        r = w.statement_execution.execute_statement(
            statement=statement,
            catalog=self.catalog_name,
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
            raise Exception(r.status.error)

        if insert_data:
            self.insert()

        return r

    def delete(self, force: bool = False):
        self.workspace_client.tables.delete(self.full_name, force=force)

    def insert(self, warehouse_id: str = None):

        from databricks.sdk.service.sql import StatementState

        w = self.workspace_client

        statement = f"INSERT INTO {self.full_name} VALUES\n"
        for row in self.data:
            statement += f"   {tuple(row)},\n"
        statement = statement[:-2]

        if warehouse_id is None:
            warehouse_id = settings.databricks_warehouse_id
        r = w.statement_execution.execute_statement(
            statement=statement,
            catalog=self.catalog_name,
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

    # def get(self):
    #     return self.workspace_client.tables.get(self.full_name)

    def select(self, limit=10, warehouse_id: str = None):

        from databricks.sdk.service.sql import StatementState

        w = self.workspace_client

        if warehouse_id is None:
            warehouse_id = settings.databricks_warehouse_id
        r = w.statement_execution.execute_statement(
            statement=f"SELECT * from {self.full_name} limit {limit}",
            # catalog=self.catalog_name,
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
            raise Exception(r.status.error)

        return r.result.data_array
