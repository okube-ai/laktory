import time
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

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
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

    @property
    def column_names(self):
        return [c.name for c in self.columns]

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

    def create(self, if_not_exists: bool = True, or_replace: bool = False):

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

        r = w.statement_execution.execute_statement(
            statement=statement,
            catalog=self.catalog_name,
            warehouse_id="faeced4d6221b239",
        )
        statement_id = r.statement_id
        state = r.status.state

        while state in [StatementState.PENDING, StatementState.RUNNING]:
            r = w.statement_execution.get_statement(statement_id)
            time.sleep(1)
            state = r.status.state

        if state != StatementState.SUCCEEDED:
            raise ValueError()

        return r

    def delete(self, force: bool = False):
        self.workspace_client.tables.delete(self.full_name, force=force)
