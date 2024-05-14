import os.path

from typing import Literal

from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.spark import SparkDataFrame
from laktory.polars import PolarsDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class FileDataSource(BaseDataSource):
    """
    Data source using disk files, such as data events (json/csv) and
    dataframe parquets. It is generally used in the context of a data pipeline.

    Attributes
    ----------
    format:
        Format of the data files
    header
        If `True`, first line of CSV files is assumed to be the column names.
    multiline
        If `True`, JSON files are parsed assuming that an object maybe be
        defined on multiple lines (as opposed to having a single object
        per line)
    read_options:
        Other options passed to `spark.read.options`
    schema_location:
        Path for files schema. If `None`, parent directory of `path` is used

    Examples
    ---------
    ```python
    from laktory import models

    source = models.FileDataSource(
        path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        format="JSON",
        as_stream=False,
    )
    # df = source.read(spark)
    ```
    """

    format: Literal["CSV", "PARQUET", "DELTA", "JSON"] = "JSON"
    header: bool = True
    multiline: bool = False
    path: str
    read_options: dict[str, str] = {}
    schema_location: str = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self.path)

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:
        if self.as_stream:
            logger.info(f"Reading {self._id} as stream")

            schema_location = self.schema_location
            if schema_location is None:
                schema_location = os.path.dirname(self.path)

            # Set reader
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", self.format)
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
            )

        else:
            logger.info(f"Reading {self._id} as static")

            # Set reader
            reader = spark.read.format(self.format)

        reader = (
            reader.option("multiLine", self.multiline)  # only apply to JSON format
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("header", self.header)  # only apply to CSV format
        )
        if self.read_options:
            reader = reader.options(**self.read_options)

        # Load
        df = reader.load(self.path)

        # Not supported by UC
        # .withColumn("file", F.input_file_name())

        return df
