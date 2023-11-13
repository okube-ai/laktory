from pydantic import model_validator
from typing import Any
from typing import Literal
from typing import Union
from typing import Callable

from laktory._logger import get_logger
from laktory.models.compute.tablejoin import TableJoin
from laktory.models.base import BaseModel
from laktory.models.sql.column import Column
from laktory.spark import Column as SparkColumn
from laktory.spark import DataFrame
from laktory.models.datasources import TableDataSource
from laktory.models.datasources import EventDataSource

logger = get_logger(__name__)


class TableBuilder(BaseModel):
    drop_source_columns: Union[bool, None] = None
    drop_duplicates: Union[bool, None] = None
    event_source: Union[EventDataSource, None] = None
    joins: list[TableJoin] = []
    pipeline_name: Union[str, None] = None
    table_source: Union[TableDataSource, None] = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    _table: Any = None
    _columns_to_build = []

    @model_validator(mode="after")
    def default_options(self) -> Any:
        # Default values
        if self.zone == "BRONZE":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        if self.zone == "SILVER":
            if self.drop_source_columns is None:
                self.drop_source_columns = True
            if self.drop_duplicates is not None:
                self.drop_duplicates = True

        if self.zone == "SILVER_STAR":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        return self

    @property
    def source(self):
        if self.event_source is not None and self.event_source.name is not None:
            return self.event_source
        elif self.table_source is not None and self.table_source.name is not None:
            return self.table_source

    @property
    def is_from_cdc(self):
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def columns(self):
        return self._table.columns

    @property
    def timestamp_key(self):
        return self._table.timestamp_key

    @property
    def primary_key(self):
        return self._table.primary_key

    @property
    def has_joins(self):
        return len(self.joins) > 0

    def get_zone_columns(self, zone, df=None):
        from laktory.spark.dataframe import has_column

        if zone == "BRONZE":
            return [
                Column(
                    **{
                        "name": "_bronze_at",
                        "type": "timestamp",
                        "spark_func_name": "current_timestamp",
                    }
                )
            ]

        elif zone == "SILVER":
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

        elif zone == "SILVER_STAR":
            return [
                Column(
                    **{
                        "name": "_silver_star_at",
                        "type": "timestamp",
                        "spark_func_name": "current_timestamp",
                    }
                )
            ]

    def read_source(self, spark) -> DataFrame:
        return self.source.read(spark)

    def build_columns(self, df, udfs=None, raise_exception=True) -> DataFrame:
        logger.info(f"Setting columns...")
        built_cols = []
        for col in self._columns_to_build:
            c = col.to_spark(df, udfs=udfs, raise_exception=raise_exception)
            if c is not None:
                df = df.withColumn(col.name, c)
                built_cols += [col]

        for c in built_cols:
            self._columns_to_build.remove(c)

        return df

    def process(self, df, udfs=None, spark=None) -> DataFrame:
        logger.info(f"Applying {self.zone} transformations")

        # Build columns
        self._columns_to_build = self.columns + self.get_zone_columns(
            zone=self.zone, df=df
        )
        column_names = [c.name for c in self._columns_to_build]
        df = self.build_columns(df, udfs=udfs, raise_exception=not self.has_joins)

        # Make joins
        for i, join in enumerate(self.joins):
            if i == 0:
                name = self.source.name
            else:
                name = "previous_join"
            join.left = TableDataSource(name=name)
            join.left._df = df
            df = join.run(spark)

            # Build remaining columns again (in case inputs are found in joins)
            df = self.build_columns(
                df, udfs=udfs, raise_exception=i == len(self.joins) - 1
            )

        # Drop source columns
        if self.drop_source_columns:
            logger.info(f"Dropping source columns...")
            df = df.select(column_names)

        # Drop duplicates
        pk = self.primary_key
        if self.drop_duplicates and pk:
            logger.info(f"Removing duplicates with {pk}")
            df = df.dropDuplicates([pk])

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
            "target": self._table.name,
            "track_history_column_list": cdc.track_history_columns,
            "track_history_except_column_list": cdc.track_history_except_columns,
        }
