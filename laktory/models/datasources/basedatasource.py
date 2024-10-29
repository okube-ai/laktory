from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.constants import DEFAULT_DFTYPE
from laktory.spark import SparkDataFrame
from laktory.spark import is_spark_dataframe
from laktory.polars import PolarsDataFrame
from laktory.polars import is_polars_dataframe
from laktory.types import AnyDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class DataFrameSample(BaseModel):
    fraction: float
    seed: Union[int, None] = None


class Watermark(BaseModel):
    """
    Definition of a spark structured streaming watermark for joining data
    streams.

    Attributes
    ----------
    column:
        Event time column name
    threshold:
        How late, expressed in seconds, the data is expected to be with
        respect to event time.

    References
    ----------
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
    """

    column: str
    threshold: str


class DataSourceCDC(BaseModel):
    """
    Defines the change data capture (CDC) properties of a data source. They are
    used to build the target using `apply_changes` method from Databricks DLT.

    Attributes
    ----------
    apply_as_deletes:
        Specifies when a CDC event should be treated as a DELETE rather than
        an upsert. To handle out-of-order data, the deleted row is temporarily
        retained as a tombstone in the underlying Delta table, and a view is
        created in the metastore that filters out these tombstones.
    apply_as_truncates:
        Specifies when a CDC event should be treated as a full table TRUNCATE.
        Because this clause triggers a full truncate of the target table, it
        should be used only for specific use cases requiring this
        functionality.
    columns:
        A subset of columns to include in the target table. Use `columns` to
        specify the complete list of columns to include.
    except_columns:
        A subset of columns to exclude in the target table.
    ignore_null_updates:
        Allow ingesting updates containing a subset of the target columns.
        When a CDC event matches an existing row and ignore_null_updates is
        `True`, columns with a null will retain their existing values in the
        target. This also applies to nested columns with a value of null. When
        ignore_null_updates is `False`, existing values will be overwritten
        with null values.
    primary_keys:
        The column or combination of columns that uniquely identify a row in
        the source data. This is used to identify which CDC events apply to
        specific records in the target table.
    scd_type:
        Whether to store records as SCD type 1 or SCD type 2.
    sequence_by:
        The column name specifying the logical order of CDC events in the
        source data. Delta Live Tables uses this sequencing to handle change
        events that arrive out of order.
    track_history_columns:
        A subset of output columns to be tracked for history in the target table.
    track_history_except_columns:
        A subset of output columns to be excluded from tracking.

    References
    ----------
    https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables
    """

    apply_as_deletes: Union[str, None] = None
    apply_as_truncates: Union[str, None] = None
    columns: Union[list[str], None] = []
    except_columns: Union[list[str], None] = []
    ignore_null_updates: Union[bool, None] = None
    primary_keys: list[str]
    scd_type: Literal[1, 2] = None
    sequence_by: str
    track_history_columns: Union[list[str], None] = None
    track_history_except_columns: Union[list[str], None] = None


class BaseDataSource(BaseModel):
    """
    Base class for building data source

    Attributes
    ----------
    as_stream:
        If `True`source is read as a streaming DataFrame.
    broadcast:
        If `True` DataFrame is broadcasted
    cdc:
        Change data capture specifications
    dataframe_type:
        Type of dataframe
    drops:
        List of columns to drop
    filter:
        SQL expression used to select specific rows from the source table
    renames:
        Mapping between the source table column names and new column names
    selects:
        Columns to select from the source table. Can be specified as a list
        or as a dictionary to rename the source columns
    watermark
        Spark structured streaming watermark specifications
    """

    as_stream: bool = False
    broadcast: Union[bool, None] = False
    cdc: Union[DataSourceCDC, None] = None
    dataframe_type: Literal["SPARK", "POLARS"] = DEFAULT_DFTYPE
    drops: Union[list, None] = None
    filter: Union[str, None] = None
    limit: Union[int, None] = None
    mock_df: Any = Field(default=None, exclude=True)
    renames: Union[dict[str, str], None] = None
    sample: Union[DataFrameSample, None] = None
    selects: Union[list[str], dict[str, str], None] = None
    watermark: Union[Watermark, None] = None
    _parent: "PipelineNode" = None

    @model_validator(mode="after")
    def options(self) -> Any:

        # Overwrite Dataframe type if mock dataframe is provided
        if is_spark_dataframe(self.mock_df):
            self.dataframe_type = "SPARK"
        elif is_polars_dataframe(self.mock_df):
            self.dataframe_type = "POLARS"

        if self.dataframe_type == "SPARK":
            pass
        elif self.dataframe_type == "POLARS":
            if self.as_stream:
                raise ValueError("Polars DataFrames don't support streaming read.")
            if self.watermark:
                raise ValueError("Polars DataFrames don't support watermarking.")
            if self.broadcast:
                raise ValueError("Polars DataFrames don't support broadcasting.")

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def user_dftype(self):
        if "dataframe_type" in self.model_fields_set:
            return self.dataframe_type
        return None

    @property
    def _id(self):
        return str(self)

    @property
    def is_cdc(self) -> bool:
        """If `True` source data is a change data capture (CDC)"""
        return self.cdc is not None

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, data source is used in the context of a DLT pipeline"""
        is_orchestrator_dlt = False
        if self._parent and self._parent.is_orchestrator_dlt:
            is_orchestrator_dlt = True
        return is_orchestrator_dlt

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark=None) -> AnyDataFrame:
        """
        Read data with options specified in attributes.

        Parameters
        ----------
        spark:
            Spark context

        Returns
        -------
        : DataFrame
            Resulting dataframe
        """
        if self.mock_df is not None:
            df = self.mock_df
        elif self.dataframe_type == "SPARK":
            df = self._read_spark(spark=spark)
        elif self.dataframe_type == "POLARS":
            df = self._read_polars()
        else:
            raise ValueError(
                f"DataFrame type '{self.dataframe_type}' is not supported."
            )

        if is_spark_dataframe(df):
            df = self._post_read_spark(df)
        elif is_polars_dataframe(df):
            df = self._post_read_polars(df)

        return df

    def _read_spark(self, spark) -> SparkDataFrame:
        raise NotImplementedError()

    def _read_polars(self) -> PolarsDataFrame:
        raise NotImplementedError()

    def _post_read_spark(self, df: SparkDataFrame) -> SparkDataFrame:

        import pyspark.sql.functions as F

        # Apply filter
        if self.filter:
            df = df.filter(self.filter)

        # Apply drops
        if self.drops:
            df = df.drop(*self.drops)

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [F.col(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [F.col(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Renames
        if self.renames:
            for old_name, new_name in self.renames.items():
                df = df.withColumnRenamed(old_name, new_name)

        # Apply Watermark
        if self.watermark:
            df = df.withWatermark(
                self.watermark.column,
                self.watermark.threshold,
            )

        # Broadcast
        if self.broadcast:
            df = F.broadcast(df)

        # Sample
        if self.sample:
            df = df.sample(fraction=self.sample.fraction, seed=self.sample.seed)

        # Limit
        if self.limit:
            df = df.limit(self.limit)

        return df

    def _post_read_polars(self, df: PolarsDataFrame) -> PolarsDataFrame:

        from laktory.polars.expressions.sql import _parse_token
        import polars as pl

        # Apply filter
        if self.filter:
            df = df.filter(pl.Expr.laktory.sql_expr(self.filter))

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [_parse_token(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [_parse_token(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Apply drops
        if self.drops:
            df = df.drop(*self.drops)

        # Renames
        if self.renames:
            df = df.rename(self.renames)

        # Apply Watermark
        if self.watermark:
            raise NotImplementedError(
                "Watermarking not supported with POLARS dataframe"
            )

        # Broadcast
        if self.broadcast:
            raise NotImplementedError(
                "Broadcasting not supported with POLARS dataframe"
            )

        # Sample
        if self.sample:
            raise NotImplementedError(
                "Broadcasting not supported with POLARS lazyframe"
            )
            # df = df.sample(fraction=self.sample.fraction, seed=self.sample.seed)

        # Limit
        if self.limit:
            df = df.limit(self.limit)

        return df
