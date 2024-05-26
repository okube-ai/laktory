from typing import Literal
from typing import Union
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.spark import SparkDataFrame
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory._logger import get_logger

logger = get_logger(__name__)


class TableDataSink(BaseDataSink):
    """
    Data Table data sink such as table on a Databricks catalog or on a
    data warehouse such as Snowflake, BigQuery, etc.

    Attributes
    ----------
    checkpoint_location:
        Path to which the checkpoint file for streaming dataframe should
        be written.
    catalog_name:
        Name of the catalog of the source table
    table_name:
        Name of the source table
    schema_name:
        Name of the schema of the source table
    warehouse:
        Type of warehouse to which the table should be published

    Examples
    ---------
    ```python
    from laktory import models
    import pandas as pd

    df = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
                "tstamp": ["2023-09-01", "2023-09-01"],
            }
        )
    )

    sink = models.TableDataSink(
        catalog_name="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        schema_name="finance",
        table_name="slv_stock_prices",
        mode="OVERWRITE",
    )
    # sink.write(df)
    ```
    """

    catalog_name: Union[str, None] = None
    checkpoint_location: Union[str, None] = None
    format: Literal["DELTA", "PARQUET"] = "DELTA"
    schema_name: Union[str, None] = None
    table_name: Union[str, None]
    warehouse: Union[Literal["DATABRICKS"], None] = "DATABRICKS"

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Table full name {catalog_name}.{schema_name}.{table_name}"""
        if self.table_name is None:
            return None

        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.schema_name is not None:
            if name == "":
                name = self.schema_name
            else:
                name += f".{self.schema_name}"

        if name == "":
            name = self.table_name
        else:
            name += f".{self.table_name}"

        return name

    @property
    def _id(self) -> str:
        return self.full_name

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df: SparkDataFrame, mode=None) -> None:

        if mode is None:
            mode = self.mode

        if self.warehouse == "DATABRICKS":
            return self._write_spark_databricks(df, mode=mode)
        else:
            raise NotImplementedError(
                f"Warehouse '{self.warehouse}' is not yet supported."
            )

    def _write_spark_databricks(self, df: SparkDataFrame, mode) -> None:

        if df.isStreaming:
            logger.info(f"Writing {self._id} as stream with mode {self.mode}")
            writer = (
                df.writeStream.outputMode(self.mode)
                .format(self.format)
                .trigger(availableNow=True)  # TODO: Add option for trigger?
                .option("mergeSchema", "true")
            )

            if self.checkpoint_location:
                writer = writer.option("checkpointLocation", self.checkpoint_location)

            writer.toTable(self.full_name)

        else:
            logger.info(f"Writing {self._id} as static with mode {self.mode}")
            (
                df.write.format(self.format)
                .mode(mode)
                .option("mergeSchema", "true")
                .saveAsTable(self.full_name)
            )

    def as_source(self, as_stream=None) -> TableDataSource:
        """
        Generate a table data source with the same properties as the sink.

        Parameters
        ----------
        as_stream:
            If `True`, sink will be read as stream.

        Returns
        -------
        :
            Table Data Source
        """
        source = TableDataSource(
            catalog_name=self.catalog_name,
            table_name=self.table_name,
            schema_name=self.schema_name,
            warehouse=self.warehouse,
        )
        if as_stream:
            source.as_stream = as_stream

        return source
