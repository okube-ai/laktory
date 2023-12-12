from laktory.spark import DataFrame
from typing import Union
from typing import Literal
from typing import Any

from laktory.models.basemodel import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
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


class TableDataSourceCDC(BaseModel):
    """
    Defines the change data capture (CDC) properties of a table data source.
    They are used to build the target using `apply_changes` method from
    Databricks DLT.

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


class TableDataSource(BaseDataSource):
    """
    Data source using a SQL table, generally used in the context of a
    data pipeline.

    Attributes
    ----------
    catalog_name:
        Name of the catalog of the source table
    cdc:
        Change data capture specifications
    selects:
        Columns to select from the source table. Can be specified as a list
        or as a dictionary to rename the source columns
    filter:
        SQL expression used to select specific rows from the source table
    from_pipeline:
        If `True` the source table will be read using `dlt.read` instead of
        `spark.read`
    name:
        Name of the source table
    schema_name:
        Name of the schema of the source table
    watermark
        Spark structured streaming watermark specifications

    Examples
    ---------
    ```python
    from laktory import models

    source = models.TableDataSource(
        name="brz_stock_prices",
        selects=["symbol", "open", "close"],
        filter="symbol='AAPL'",
        from_pipeline=False,
        read_as_stream=True,
    )
    # df = source.read(spark)
    ```
    """

    _df: Any = None
    catalog_name: Union[str, None] = None
    cdc: Union[TableDataSourceCDC, None] = None
    selects: Union[list[str], dict[str, str], None] = None
    filter: Union[str, None] = None
    from_pipeline: Union[bool, None] = True
    name: Union[str, None]
    schema_name: Union[str, None] = None
    watermark: Union[Watermark, None] = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.schema_name is not None:
            if name == "":
                name = self.schema_name
            else:
                name += f".{self.schema_name}"

        if name == "":
            name = self.name
        else:
            name += f".{self.name}"

        return name

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read(self, spark) -> DataFrame:
        from laktory.dlt import read
        from laktory.dlt import read_stream

        if self._df is not None:
            logger.info(f"Reading {self.full_name} from memory")
            df = self._df
        elif self.read_as_stream:
            logger.info(f"Reading {self.full_name} as stream")
            if self.from_pipeline:
                df = read_stream(self.full_name)
            else:
                df = spark.readStream.format("delta").table(self.full_name)
        else:
            logger.info(f"Reading {self.full_name} as static")
            if self.from_pipeline:
                df = read(self.full_name)
            else:
                df = spark.read.table(self.full_name)

        return df

    def read(self, spark) -> DataFrame:
        import pyspark.sql.functions as F

        df = self._read(spark)

        # Apply filter
        if self.filter:
            df = df.filter(self.filter)

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [F.col(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [F.col(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Apply Watermark
        if self.watermark:
            df = df.withWatermark(
                self.watermark.column,
                self.watermark.threshold,
            )

        return df


if __name__ == "__main__":
    from laktory import models

    source = models.TableDataSource(
        name="brz_stock_prices",
        selects=["symbol", "open", "close"],
        filter="symbol='AAPL'",
        from_pipeline=False,
        read_as_stream=True,
    )
    df = source.read(spark)
