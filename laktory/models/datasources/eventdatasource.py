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
    fmt
        Format of the stored data
    header
        If `True`, first line of CSV files is assumed to be the column names.
    multiline
        If `True`, JSON files are parsed assuming that an object maybe be
        defined on multiple lines (as opposed to having a single object
        per line)
    read_options:
        Other options passed to `spark.read.options`
    schema_location:
        Path for events schema. If `None`, `event_root` is used.
    type
        Type of infrastructure storing the data

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

    fmt: Literal["JSON", "CSV", "PARQUET", "DELTA"] = "JSON"
    header: bool = True
    multiline: bool = False
    read_options: dict[str, str] = {}
    schema_location: str = None
    type: Literal["STORAGE_EVENTS", "STREAM_KINESIS", "STREAM_KAFKA"] = "STORAGE_EVENTS"

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_storage(self, spark) -> DataFrame:
        if self.read_as_stream:
            logger.info(f"Reading {self.event_root} as stream")

            schema_location = self.schema_location
            if schema_location is None:
                schema_location = self.event_root

            # Set reader
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", schema_location)
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
