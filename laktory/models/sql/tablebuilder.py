from pydantic import model_validator
from typing import Any
from typing import Literal
from typing import Union
from typing import Callable

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.datasources import EventDataSource
from laktory.models.datasources import TableDataSource
from laktory.models.sql.column import Column
from laktory.models.sql.tableaggregation import TableAggregation
from laktory.models.sql.tablejoin import TableJoin
from laktory.models.sql.tablewindowfilter import TableWindowFilter
from laktory.spark import DataFrame

logger = get_logger(__name__)


class TableBuilder(BaseModel):
    """
    Mechanisms for building a table from a source in the context of a data
    pipeline.

    Attributes
    ----------
    aggregation:
        Definition of the aggregation model following the joins and the source
        read.
    drop_columns:
        Columns to drop from the output dataframe
    drop_duplicates:
        If `True`, drop duplicated rows using table `primary_key`
    drop_source_columns:
        If `True`, drop columns from the source after read and only keep
        columns defined in the table and/or resulting from the joins.
    event_source:
        Definition of the event data source if applicable
    filter:
        Filter applied to the data source to keep only selected rows
    joins:
        Definition of the table(s) to be joined to the data source
    joins_post_aggregation
        Definition of the table(s) to be joined to the aggregated data
    layer:
        Layer in the medallion architecture
    pipeline_name:
        Name of the pipeline in which the table will be built
    selects:
        Columns to select from the output dataframe. A list of colum names or
        a map for renaming the columns.
    table_source:
        Definition of the table data source if applicable
    template:
        Key indicating which notebook to use for building the table in the
        context of a data pipeline.
    window_filter:
        Definition of rows filter based on a time spark window. Applied after
        joins.
    """

    aggregation: Union[TableAggregation, None] = None
    drop_columns: list[str] = []
    drop_duplicates: Union[bool, None] = None
    drop_source_columns: Union[bool, None] = None
    event_source: Union[EventDataSource, None] = None
    filter: Union[str, None] = None
    joins: list[TableJoin] = []
    joins_post_aggregation: list[TableJoin] = []
    layer: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    pipeline_name: Union[str, None] = None
    selects: Union[list[str], dict[str, str], None] = None
    table_source: Union[TableDataSource, None] = None
    template: Union[str, bool, None] = None
    window_filter: Union[TableWindowFilter, None] = None
    _columns_to_build = []
    _table: Any = None

    @model_validator(mode="after")
    def default_options(self) -> Any:
        """
        Sets default options like `drop_source_columns`, `drop_duplicates`,
        `template`, etc. based on `layer` value.
        """
        # Default values
        if self.layer == "BRONZE":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        if self.layer == "SILVER":
            if self.drop_source_columns is None:
                self.drop_source_columns = True
            if self.drop_duplicates is not None:
                self.drop_duplicates = True

        if self.layer == "SILVER_STAR":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        if self.layer == "GOLD":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        if self.template is None:
            self.template = self.layer

        return self

    @property
    def source(self) -> BaseDataSource:
        """Selected data source"""
        if self.event_source is not None and self.event_source.name is not None:
            return self.event_source
        elif self.table_source is not None and self.table_source.name is not None:
            return self.table_source

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def columns(self) -> list[Column]:
        """List of columns"""
        return self._table.columns

    @property
    def timestamp_key(self) -> str:
        """Table timestamp key"""
        return self._table.timestamp_key

    @property
    def primary_key(self) -> str:
        """Table primary key"""
        return self._table.primary_key

    @property
    def has_joins(self) -> bool:
        """Joins defined flag"""
        return len(self.joins) > 0

    @property
    def has_joins_post_aggregation(self) -> bool:
        """Post-aggregation joins defined flag"""
        return len(self.joins_post_aggregation) > 0

    def _get_layer_columns(self, layer, df=None) -> list[columns]:
        from laktory.spark.dataframe import has_column

        cols = []

        if layer == "BRONZE":
            cols = [
                Column(
                    **{
                        "name": "_bronze_at",
                        "type": "timestamp",
                        "spark_func_name": "current_timestamp",
                    }
                )
            ]

        elif layer == "SILVER":
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

        elif layer == "SILVER_STAR":
            cols = [
                Column(
                    **{
                        "name": "_silver_star_at",
                        "type": "timestamp",
                        "spark_func_name": "current_timestamp",
                    }
                )
            ]

        elif layer == "GOLD":
            cols = [
                Column(
                    **{
                        "name": "_gold_at",
                        "type": "timestamp",
                        "spark_func_name": "current_timestamp",
                    }
                )
            ]

        return cols

    def read_source(self, spark) -> DataFrame:
        """
        Read data source specified in `self.source`

        Parameters
        ----------
        spark: SparkSession
            Spark session

        Returns
        -------
        :
            Output dataframe
        """
        return self.source.read(spark)

    def build_columns(
        self, df: DataFrame, udfs: list[Callable] = None, raise_exception: bool = True
    ) -> DataFrame:
        """
        Build dataframe columns

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions
        raise_exception
            If `True`, raise exception when input columns are not available,
            else, skip.
        """
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
        """
        Build table from source DataFrame by applying joins, aggregations and
        creating new columns.

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions
        spark: SparkSession
            Spark sessions.

        Returns
        -------
        :
            output Spark DataFrame
        """
        import pyspark.sql.functions as F

        logger.info(f"Applying {self.layer} transformations")

        # Build columns
        self._columns_to_build = self.columns + self._get_layer_columns(
            layer=self.layer, df=df
        )
        column_names = [c.name for c in self._columns_to_build]
        df = self.build_columns(
            df, udfs=udfs, raise_exception=not (self.has_joins or self.aggregation)
        )

        # Make joins
        for i, join in enumerate(self.joins):
            if i == 0:
                name = self.source.name
            else:
                name = "previous_join"
            join.left = TableDataSource(name=name)
            join.left._df = df
            df = join.execute(spark)

            # Build remaining columns again (in case inputs are found in joins)
            df = self.build_columns(
                df, udfs=udfs, raise_exception=i == len(self.joins) - 1
            )

        # Window filtering
        if self.window_filter:
            df = self.window_filter.execute(df)

        # Drop source columns
        if self.drop_source_columns:
            logger.info(f"Dropping source columns...")
            df = df.select(column_names)

        if self.aggregation:
            df = self.aggregation.execute(df, udfs=udfs)
            self._columns_to_build += self._get_layer_columns(layer=self.layer, df=df)

        # Build columns after aggregation
        df = self.build_columns(
            df, udfs=udfs, raise_exception=not self.has_joins_post_aggregation
        )

        # Make post-aggregation joins
        for i, join in enumerate(self.joins_post_aggregation):
            if i == 0:
                name = self.source.name
            else:
                name = "previous_join"
            join.left = TableDataSource(name=name)
            join.left._df = df
            df = join.execute(spark)

            # Build remaining columns again (in case inputs are found in joins)
            df = self.build_columns(
                df, udfs=udfs, raise_exception=i == len(self.joins) - 1
            )

        # Apply filter
        if self.filter:
            df = df.filter(self.filter)

        # Select columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [F.col(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [F.col(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Drop columns
        if self.drop_columns:
            logger.info(f"Dropping columns {self.drop_columns}...")
            df = df.drop(*self.drop_columns)

        # Drop duplicates
        pk = self.primary_key
        if self.drop_duplicates and pk:
            logger.info(f"Removing duplicates with {pk}")
            df = df.dropDuplicates([pk])

        return df

    @property
    def apply_changes_kwargs(self) -> dict[str, str]:
        """Keyword arguments for dlt.apply_changes function"""
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
