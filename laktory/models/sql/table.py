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


class TableSCD(BaseModel):
    columns: Union[list[str], None] = []
    except_columns: Union[list[str], None] = []
    track_history_columns: Union[list[str], None] = None
    track_history_except_columns: Union[list[str], None] = None
    type: Literal[1, 2] = None


class Table(BaseModel):
    catalog_name: Union[str, None] = None
    columns: list[Column] = []
    comment: Union[str, None] = None
    data: list[list[Any]] = None
    event_source: Union[EventDataSource, None] = None
    grants: list[TableGrant] = None
    name: str
    pipeline_name: Union[str, None] = None
    primary_key: Union[str, None] = None
    scd: Union[TableSCD, None] = None
    schema_name: Union[str, None] = None
    table_source: Union[TableDataSource, None] = None
    timestamp_key: Union[str, None] = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
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

        if self.scd is not None:
            if self.table_source is None or self.table_source.cdc is None:
                raise ValueError("For table to be set as SCD, the source must be a table with `cdc` field defined.")

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

        from laktory.spark.dataframe import has_column

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
        if has_column(df, "_bronze_at"):
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

    @property
    def apply_changes_kwargs(self):
        cdc = self.source.cdc
        scd = self.scd
        return {
            "apply_as_deletes": cdc.apply_as_deletes,
            "apply_as_truncates": cdc.apply_as_truncates,
            "column_list": scd.columns,
            "except_column_list": scd.except_columns,
            "ignore_null_updates": cdc.ignore_null_updates,
            "keys": cdc.primary_keys,
            "sequence_by": cdc.sequence_by,
            "source": self.source.name,
            "stored_as_scd_type": scd.type,
            "target": self.name,
            "track_history_column_list": scd.track_history_columns,
            "track_history_except_column_list": scd.track_history_except_columns,
        }
