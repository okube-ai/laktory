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
from laktory.models.sql.tablejoin import TableJoin

logger = get_logger(__name__)


class Table(BaseModel):
    catalog_name: Union[str, None] = None
    columns: list[Column] = []
    comment: Union[str, None] = None
    data: list[list[Any]] = None
    event_source: Union[EventDataSource, None] = None
    grants: list[TableGrant] = None
    joins: list[TableJoin] = []
    name: str
    pipeline_name: Union[str, None] = None
    primary_key: Union[str, None] = None
    schema_name: Union[str, None] = None
    table_source: Union[TableDataSource, None] = None
    timestamp_key: Union[str, None] = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    _columns_to_build = []
    # joins
    # expectations

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode="after")
    def assign_catalog_schema(self) -> Any:
        # Assign to columns
        for c in self.columns:
            c.table_name = self.name
            c.catalog_name = self.catalog_name
            c.schema_name = self.schema_name

        # Assign to sources
        if self.table_source is not None:
            if self.table_source.catalog_name is None:
                self.table_source.catalog_name = self.catalog_name
            if self.table_source.schema_name is None:
                self.table_source.schema_name = self.schema_name

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

    def to_df(self, spark=None):
        import pandas as pd

        df = pd.DataFrame(data=self.data, columns=self.column_names)

        if spark:
            df = spark.createDataFrame(df)
        return df

    @property
    def source(self):
        if self.event_source is not None and self.event_source.name is not None:
            return self.event_source
        elif self.table_source is not None and self.table_source.name is not None:
            return self.table_source

    # ----------------------------------------------------------------------- #
    # Pipeline Methods                                                        #
    # ----------------------------------------------------------------------- #

    def get_bronze_columns(self):
        return [
            Column(
                **{
                    "name": "_bronze_at",
                    "type": "timestamp",
                    "spark_func_name": "current_timestamp",
                }
            )
        ]

    def get_silver_columns(self, df):
        from laktory.spark.dataframe import has_column

        cols = []

        if self.timestamp_key:
            cols += [
                Column(
                    **{
                        "name": "_tstamp",
                        "type": "timestamp",
                        "spark_func_name": "coalesce",
                        "spark_func_args": [self.timestamp_key],
                    }
                )
            ]

        if has_column(df, "_bronze_at"):
            cols += [
                Column(
                    **{
                        "name": "_bronze_at",
                        "type": "timestamp",
                        "spark_func_name": "coalesce",
                        "spark_func_args": ["_bronze_at"],
                    }
                )
            ]

        cols += [
            Column(
                **{
                    "name": "_silver_at",
                    "type": "timestamp",
                    "spark_func_name": "current_timestamp",
                }
            )
        ]

        return cols

    def get_silver_star_columns(self):
        return [
            Column(
                **{
                    "name": "_silver_star_at",
                    "type": "timestamp",
                    "spark_func_name": "current_timestamp",
                }
            )
        ]

    @property
    def is_from_cdc(self):
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    def read_source(self, spark) -> DataFrame:
        return self.source.read(spark)

    def build_columns(self, df, udfs=None, raise_exception=True) -> DataFrame:
        logger.info(f"Setting columns...")
        built_cols = []
        for col in self._columns_to_build:
            print("col...", col.name, [c.name for c in self._columns_to_build])
            c = col.to_spark(df, udfs=udfs, raise_exception=raise_exception)
            if c is not None:
                df = df.withColumn(col.name, c)
                built_cols += [col]

        for c in built_cols:
            self._columns_to_build.remove(c)

        return df

    def process_bronze(self, df) -> DataFrame:
        logger.info(f"Applying bronze transformations")

        # Build columns
        self._columns_to_build = self.columns + self.get_bronze_columns()
        df = self.build_columns(df)

        return df

    def process_silver(
        self, df, udfs: list[Callable[[...], SparkColumn]] = None
    ) -> DataFrame:
        logger.info(f"Applying silver transformations")

        # Build columns
        self._columns_to_build = self.columns + self.get_silver_columns(df)
        print("columns to buid", [c.name for c in self._columns_to_build])
        column_names = [c.name for c in self._columns_to_build]
        df = self.build_columns(df, udfs=udfs)

        # Drop previous columns
        logger.info(f"Dropping bronze columns...")
        df = df.select(column_names)

        # Drop duplicates
        pk = self.primary_key
        if pk:
            logger.info(f"Removing duplicates with {pk}")
            df = df.dropDuplicates([pk])

        return df

    def process_silver_star(
        self, df, udfs: list[Callable[[...], SparkColumn]] = None, spark: Any = None
    ) -> DataFrame:
        logger.info(f"Applying silver star transformations")

        # Build columns
        self._columns_to_build = self.columns + self.get_silver_star_columns()
        df = self.build_columns(df, udfs=udfs, raise_exception=False)

        for i, join in enumerate(self.joins):
            join.left = self.source
            join.left._df = df
            # TODO: Review if required / desirable
            if i > 0:
                join.left.watermark = self.joins[i - 1].other.watermark
            print("SPARK!!", spark)
            df = join.run(spark)
            df.printSchema()

            # Build columns
            df = self.build_columns(
                df, udfs=udfs, raise_exception=i == len(self.joins) - 1
            )

        return df

    @property
    def apply_changes_kwargs(self):
        cdc = self.source.cdc
        return {
            "apply_as_deletes": cdc.apply_as_deletes,
            "apply_as_truncates": cdc.apply_as_truncates,
            "column_list": cdc.columns,
            "except_column_list": cdc.except_columns,
            "ignore_null_updates": cdc.ignore_null_updates,
            "keys": cdc.primary_keys,
            "sequence_by": cdc.sequence_by,
            "source": self.source.name,
            "stored_as_scd_type": cdc.scd_type,
            "target": self.name,
            "track_history_column_list": cdc.track_history_columns,
            "track_history_except_column_list": cdc.track_history_except_columns,
        }
