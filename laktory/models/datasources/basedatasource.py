from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator

from laktory.models.basemodel import BaseModel

# from laktory.models.spark.sparkchain import SparkChain
from laktory.spark import SparkDataFrame
from laktory.spark import is_spark_dataframe
from laktory.polars import PolarsDataFrame
from laktory.polars import is_polars_dataframe
from laktory.types import AnyDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


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
    dataframe_type:
        Type of dataframe
    cdc:
        Change data capture specifications
    drops:
        List of columns to drop
    selects:
        Columns to select from the source table. Can be specified as a list
        or as a dictionary to rename the source columns
    filter:
        SQL expression used to select specific rows from the source table
    read_as_stream
        If `True` read source as stream
    renames:
        Mapping between the source table column names and new column names
    watermark
        Spark structured streaming watermark specifications

    """

    as_stream: bool = False
    broadcast: Union[bool, None] = False
    cdc: Union[DataSourceCDC, None] = None
    dataframe_type: Literal["SPARK", "POLARS"] = "SPARK"
    drops: Union[list, None] = None
    filter: Union[str, None] = None
    # mock_df: Any = Field(default=None, exclude=True)
    renames: Union[dict[str, str], None] = None
    selects: Union[list[str], dict[str, str], None] = None
    # spark_chain: Union[SparkChain, None] = None
    watermark: Union[Watermark, None] = None

    @model_validator(mode="after")
    def options(self) -> Any:

        dataframe_type = self.dataframe_type
        # if is_spark_dataframe(self.mock_df):
        #     dataframe_type = "SPARK"
        # elif is_polars_dataframe(self.mock_df):
        #     dataframe_type = "POLARS"

        if dataframe_type == "SPARK":
            pass
        elif dataframe_type == "POLARS":
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
    def _id(self):
        return str(self)

    @property
    def is_cdc(self) -> bool:
        """If `True` source data is a change data capture (CDC)"""
        return self.cdc is not None

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark=None) -> AnyDataFrame:
        """
        Read data with options specified in attributes.

        Parameters
        ----------
        spark
            Spark context

        Returns
        -------
        : DataFrame
            Resulting dataframe
        """
        if self.dataframe_type == "SPARK":
            return self.read_spark(spark=spark)
        elif self.dataframe_type == "POLARS":
            return self.read_polars()
        else:
            raise ValueError(
                f"DataFrame type '{self.dataframe_type}' is not supported."
            )

    def read_spark(self, spark) -> SparkDataFrame:
        """
        Read data as Spark DataFrame with options specified in attributes.

        Parameters
        ----------
        spark
            Spark context

        Returns
        -------
        : DataFrame
            Spark dataframe
        """
        df = self._read_spark(spark=spark)
        df = self._post_read_spark(df)
        return df

    def read_polars(self) -> PolarsDataFrame:
        """
        Read data as Polars DataFrame with options specified in attributes.

        Returns
        -------
        : DataFrame
            Polars dataframe
        """
        df = self._read_polars()
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

        # SparkChain
        # if self.spark_chain:
        #     df = self.spark_chain.execute(df, udfs=None, spark=df.sparkSession)

        return df

    def _post_read_polars(self, df: PolarsDataFrame) -> PolarsDataFrame:
        raise NotImplementedError()
