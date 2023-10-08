import json
from typing import Literal
from typing import Any
from typing import Union

from pydantic import computed_field
from pydantic import model_validator

from laktory.spark import DataFrame
from laktory.spark import df_has_column

from laktory._logger import get_logger
from laktory.sql import py_to_sql
from laktory.models.base import BaseModel
from laktory.models.column import Column
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.models.datasources.eventdatasource import EventDataSource
from laktory.models.grants.tablegrant import TableGrant

logger = get_logger(__name__)


class Table(BaseModel):
    name: str
    columns: list[Column] = []
    primary_key: Union[str, None] = None
    comment: Union[str, None] = None
    catalog_name: Union[str, None] = None
    schema_name: Union[str, None] = None
    grants: list[TableGrant] = None

    # Data
    data: list[list[Any]] = None

    # Lakehouse
    timestamp_key: Union[str, None] = None
    event_source: Union[EventDataSource, None] = None
    table_source: Union[TableDataSource, None] = None
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
            c.schema_name = self.schema_name
        return self

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        return _id

    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @property
    def database_name(self) -> str:
        return self.schema_name

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
            schema_name="laktory",
            columns=columns,
        )

    # ----------------------------------------------------------------------- #
    # SQL Methods                                                             #
    # ----------------------------------------------------------------------- #

    def exists(self):
        return self.name in [
            c.name
            for c in self.workspace_client.tables.list(
                catalog_name=self.catalog_name,
                schema_name=self.schema_name,
            )
        ]

    def create(
        self,
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
            statement, warehouse_id=warehouse_id, catalog_name=self.catalog_name
        )

        if insert_data:
            self.insert()

        return r

    def delete(self):
        self.workspace_client.tables.delete(self.full_name)

    def insert(self, warehouse_id: str = None):
        if self.data is None or len(self.data) == 0:
            return

        statement = f"INSERT INTO {self.full_name} VALUES\n"
        for row in self.data:
            statement += "   ("
            values = [
                json.dumps(v) if c.jsonize else v for c, v in zip(self.columns, row)
            ]
            values = [py_to_sql(v) for v in values]
            statement += ", ".join(values)
            statement += "),\n"

        statement = statement[:-2]

        logger.info(statement)
        r = self.workspace_client.execute_statement_and_wait(
            statement, warehouse_id=warehouse_id, catalog_name=self.catalog_name
        )

        return r

    def select(self, limit=10, warehouse_id: str = None, load_json=True):
        statement = f"SELECT * from {self.full_name} limit {limit}"

        r = self.workspace_client.execute_statement_and_wait(
            statement, warehouse_id=warehouse_id, catalog_name=self.catalog_name
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
        import pyspark.sql.functions as F

        logger.info(f"Applying bronze transformations")

        df = df.withColumn("_bronze_at", F.current_timestamp())

        return df

    def process_silver(self, df, table) -> DataFrame:
        import pyspark.sql.functions as F

        logger.info(f"Applying silver transformations")

        columns = []

        # User defined columns
        columns += table.columns

        if table.timestamp_key is not None:
            columns += [
                Column(
                    **{
                        "name": "_tstamp",
                        "type": "timestamp",
                        "func_name": "coalesce",
                        "input_cols": [table.timestamp_key],
                    }
                )
            ]

        # Timestamps
        if df_has_column(df, "bronze_at"):
            columns += [
                Column(
                    **{
                        "name": "_bronze_at",
                        "type": "timestamp",
                        "func_name": "coalesce",
                        "input_cols": ["_bronze_at", "bronze_at"],
                    }
                )
            ]
        columns += [
            Column(
                **{
                    "name": "_silver_at",
                    "type": "timestamp",
                    "func_name": "current_timestamp",
                    "input_cols": [],
                }
            )
        ]

        # Saved existing column names
        cols0 = [v[0] for v in df.dtypes]

        # Build new columns
        logger.info(f"Setting silver columns...")
        new_col_names = []
        for col in columns:
            # Get column definition
            col_name = col.name
            col_type = col.type
            func_name = col.func_name

            #     # TODO: Support customer functions
            #     # if udf_name in udfuncs.keys():
            #     #     f = udfuncs[udf_name]
            #     # else:
            #     #     f = getattr(F, udf_name)

            f = getattr(F, func_name)
            new_col_names += [col_name]

            input_cols = col.input_cols
            kwargs = col.func_kwargs

            #     # Issue when setting strings
            #     # if "format" in kwargs and udf_name == "date_format":
            #     #     kwargs["format"] = kwargs["format"].replace("T", "'T'").replace("''T''", "'T'")

            logger.info(
                f"   {col_name}[{col_type}] as {func_name}({input_cols}, {kwargs})"
            )

            # Cast inputs
            tmp_input_cols = []
            for i, input_col in enumerate(input_cols):
                if func_name == "coalesce" and not df_has_column(df, input_col):
                    # Because the `coalesce` function supports multiple
                    # optional input columns, we can skip the ones not
                    # available in the dataframe
                    logger.warning(f"Column '{input_col}' not available")
                    continue

                tmp_input_cols += [f"__{i}"]
                _icol = tmp_input_cols[-1]
                df = df.withColumn(_icol, F.expr(input_col))
                if input_col.startswith("data.") or func_name == "coalesce":
                    input_type = dict(df.dtypes)[_icol]
                    if input_type in ["double"]:
                        # Some bronze NaN data will be converted to 0 if cast to int
                        sdf = df.withColumn(
                            _icol, F.when(F.isnan(_icol), None).otherwise(F.col(_icol))
                        )
                    if col_type not in ["_any"] and func_name not in [
                        "to_safe_timestamp"
                    ]:
                        df = df.withColumn(_icol, F.col(_icol).cast(col_type))

            # Set output
            df = df.withColumn(
                col_name,
                f(
                    *tmp_input_cols,
                    **kwargs,
                ),
            )

            # Drop temp inputs
            if len(tmp_input_cols) > 0:
                df = df.drop(*tmp_input_cols)

            # Remove from drop list
            if col_name in cols0:
                cols0.remove(col_name)

        # Drop previous columns
        logger.info(f"Dropping bronze columns...")
        df = df.select(new_col_names)

        # ------------------------------------------------------------------- #
        # Setting Watermark                                                   #
        # ------------------------------------------------------------------- #

        # TODO:
        # if watermark is not None:
        #     sdf = sdf.withWatermark(watermark["column"], watermark["threshold"])

        # ------------------------------------------------------------------- #
        # Drop duplicates                                                     #
        # ------------------------------------------------------------------- #

        pk = table.primary_key
        if pk:
            logger.info(f"Removing duplicates with {pk}")
            df = df.dropDuplicates([pk])

        return df

    def process_silver_star(self, df) -> DataFrame:
        return df
