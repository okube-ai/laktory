from pydantic import model_validator
from laktory.spark import DataFrame
from typing import Literal
from typing import Any

from laktory.models.dataeventheader import DataEventHeader
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory._logger import get_logger

logger = get_logger(__name__)


class EventDataSource(BaseDataSource, DataEventHeader):
    """
    Data source using events data (files), generally used in the context of a
    data pipeline.

    Attributes
    ----------
    type
        Type of infrastructure storing the data
    fmt
        Format of the stored data
    multiline
        If `True`, JSON files are parsed assuming that an object maybe be
        defined on multiple lines (as opposed to having a single object
        per line)
    header
        If `True`, first line of CSV files is assumed to be the column names.
    read_options:
        Other options passed to `spark.read.options`

    Examples
    ---------
    ```python
    from laktory import models

    source = models.EventDataSource(
        name="stock_price",
        producer={"name": "yahoo-finance"},
        fmt="JSON",
        read_as_stream=False,
    )
    # df = source.read(spark)
    ```
    """

    type: Literal["STORAGE_EVENTS", "STREAM_KINESIS", "STREAM_KAFKA"] = "STORAGE_EVENTS"
    fmt: Literal["JSON", "CSV", "PARQUET"] = "JSON"
    multiline: bool = False
    header: bool = True
    read_options: dict[str, str] = {}

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_storage(self, spark) -> DataFrame:
        if self.read_as_stream:
            logger.info(f"Reading {self.event_root} as stream")

            # Set reader
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self.event_root)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
            )

        else:
            logger.info(f"Reading {self.event_root} as static")

            # Set reader
            reader = spark.read.format(self.fmt)

        reader = (
            reader.option("multiLine", self.multiline)  # only apply to JSON format
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("header", self.header)  # only apply to CSV format
        )
        if self.read_options:
            reader = reader.options(**self.read_options)

        # Load
        df = reader.load(self.event_root)

        # Not supported by UC
        # .withColumn("file", F.input_file_name())

        return df

    def read(self, spark) -> DataFrame:
        """
        Read data with options specified in attributes.

        Parameters
        ----------
        spark
            Spark context

        Returns
        -------
        : DataFrame
            Resulting park dataframe
        """
        if self.type == "STORAGE_EVENTS":
            return self._read_storage(spark)
        else:
            raise NotImplementedError()
