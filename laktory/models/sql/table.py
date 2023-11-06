import json
from typing import Literal
from typing import Any
from typing import Union
from typing import Callable

from pydantic import model_validator

from laktory.spark import DataFrame

from laktory._logger import get_logger
from laktory.models.base import BaseModel
from laktory.models.sql.column import Column
from laktory.spark import Column as SparkColumn
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
    # Pipeline Methods                                                        #
    # ----------------------------------------------------------------------- #

    def read_source(self, spark) -> DataFrame:
        return self.source.read(spark)

    def process_bronze(self, df) -> DataFrame:
        import pyspark.sql.functions as F

        logger.info(f"Applying bronze transformations")

        df = df.withColumn("_bronze_at", F.current_timestamp())

        return df

    def process_silver(
        self, df, udfs: list[Callable[[...], SparkColumn]] = None
    ) -> DataFrame:
        logger.info(f"Applying silver transformations")

        columns = []

        # User defined columns
        columns += self.columns

        if self.timestamp_key is not None:
            columns += [
                Column(
                    **{
                        "name": "_tstamp",
                        "type": "timestamp",
                        "spark_func_name": "coalesce",
                        "spark_func_args": [self.timestamp_key],
                    }
                )
            ]

        # Timestamps
        if df.has_column("_bronze_at"):
            columns += [
                Column(
                    **{
                        "name": "_bronze_at",
                        "type": "timestamp",
                        "spark_func_name": "coalesce",
                        "spark_func_args": ["_bronze_at"],
                    }
                )
            ]
        columns += [
            Column(
                **{
                    "name": "_silver_at",
                    "type": "timestamp",
                    "spark_func_name": "current_timestamp",
                }
            )
        ]

        # Saved existing column names
        cols0 = [v[0] for v in df.dtypes]

        # Build new columns
        logger.info(f"Setting silver columns...")
        new_col_names = []
        for col in columns:
            # Add to list
            new_col_names += [col.name]

            # Set
            df = df.withColumn(col.name, col.to_spark(df, udfs=udfs))

            # Remove from drop list
            if col.name in cols0:
                cols0.remove(col.name)

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

        pk = self.primary_key
        if pk:
            logger.info(f"Removing duplicates with {pk}")
            df = df.dropDuplicates([pk])

        return df

    def process_silver_star(self, df) -> DataFrame:
        return df
