import json
from typing import Literal
from typing import Any
from typing import Union

from pydantic import computed_field
from pydantic import model_validator

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from laktory._logger import get_logger
from laktory.sql import py_to_sql
from laktory.models.base import BaseModel
from laktory.models.column import Column
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.models.datasources.eventdatasource import EventDataSource

logger = get_logger(__name__)


class Table(BaseModel):
    name: str
    columns: list[Column] = []
    primary_key: Union[str, None] = None
    comment: Union[str, None] = None
    catalog_name: Union[str, None] = None
    database_name: Union[str, None] = None

    # Data
    data: list[list[Any]] = None

    # Lakehouse
    timestamp_key: Union[str, None] = None
    event_source: EventDataSource = None
    table_source: TableDataSource = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    pipeline_name: Union[str, None] = None
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
        return self

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

    @property
    def source(self):
        if self.event_source is not None and self.event_source.name is not None:
            return self.event_source
        elif self.table_source is not None and self.table_source.name is not None:
            return self.table_source

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    @classmethod
    def meta_table(cls):

        # Build columns
        columns = []
        for k, t in cls.model_serialized_types().items():
            jsonize = False
            # if k in ["columns", "event_source", "table_source"]:
            if k in ["columns"]:
                t = "string"
                jsonize = True

            elif k in ["data"]:
                continue

            columns += [
                Column(name=k, type=py_to_sql(t, mode="schema"), jsonize=jsonize)
            ]

        # Set table
        return Table(
            name="tables",
            database_name="laktory",
            columns=columns,
        )

    # ----------------------------------------------------------------------- #
    # SQL Methods                                                             #
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

        if len(self.columns) == 0:
            raise ValueError()

        if or_replace:
            if_not_exists = False

        statement = f"CREATE "
        if or_replace:
            statement += "OR REPLACE "
        statement += "TABLE "
        if if_not_exists:
            statement += "IF NOT EXISTS "

        statement += f"{self.schema_name}.{self.name}"

        statement += "\n   ("
        for c in self.columns:
            t = c.type
            if c.jsonize:
                t = "string"
            statement += f"{c.name} {t},"
        statement = statement[:-1]
        statement += ")"

        logger.info(statement)
        r = self.workspace_client.execute_statement_and_wait(
            statement,
            warehouse_id=warehouse_id,
            catalog_name=self.catalog_name
        )

        if insert_data:
            self.insert()

        return r

    def delete(self):
        self.workspace_client.tables.delete(self.full_name)

    def insert(self, warehouse_id: str = None):

        statement = f"INSERT INTO {self.full_name} VALUES\n"
        for row in self.data:
            statement += "   ("
            values = [json.dumps(v) if c.jsonize else v for c, v in zip(self.columns, row)]
            values = [py_to_sql(v) for v in values]
            statement += ", ".join(values)
            statement += "),\n"

        statement = statement[:-2]

        logger.info(statement)
        r = self.workspace_client.execute_statement_and_wait(
            statement,
            warehouse_id=warehouse_id,
            catalog_name=self.catalog_name
        )

        return r

    def select(self, limit=10, warehouse_id: str = None, load_json=True):

        statement = f"SELECT * from {self.full_name} limit {limit}"

        r = self.workspace_client.execute_statement_and_wait(
            statement,
            warehouse_id=warehouse_id,
            catalog_name=self.catalog_name
        )

        data = r.result.data_array

        if load_json:
            for i in range(len(data)):
                j = -1
                for c, v in zip(self.columns, data[i]):
                    j += 1
                    if c.jsonize:
                        data[i][j] = json.loads(data[i][j])

        return data

    # ----------------------------------------------------------------------- #
    # Pipeline Methods                                                        #
    # ----------------------------------------------------------------------- #

    def read_source(self, spark) -> DataFrame:
        return self.source.read(spark)

    def process_bronze(self, df) -> DataFrame:
        df = df.withColumn("bronze_at", F.current_timestamp())
        return df

    def process_silver(self, df) -> DataFrame:
        df = df.withColumn("silver_at", F.current_timestamp())
        return df

    def process_silver_star(self, df) -> DataFrame:
        return df
