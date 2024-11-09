from typing import Union
import os.path

from typing import Literal
from typing import Any
from pydantic import model_validator

from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.spark import SparkDataFrame
from laktory.polars import PolarsLazyFrame
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
    schema:
        Target schema
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

    format: Literal["CSV", "PARQUET", "DELTA", "JSON", "EXCEL", "BINARYFILE"] = "JSON"
    path: str
    read_options: dict[str, str] = {}
    schema: Union[str, dict, list] = None
    schema_location: str = None

    @model_validator(mode="after")
    def options(self) -> Any:

        if self.dataframe_type == "SPARK":
            if self.format in [
                "EXCEL",
            ]:
                raise ValueError(f"'{self.format}' format is not supported with Spark")

        elif self.dataframe_type == "POLARS":
            if self.format in [
                "BINARYFILE",
            ]:
                raise ValueError(f"'{self.format}' format is not supported with Polars")

        return self

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

        _options = {}
        _mode = "stream"

        if self.as_stream:
            _mode = "stream"

            if self.format == "DELTA":
                reader = spark.readStream.format(self.format)

            else:
                reader = (
                    spark.readStream.format("cloudFiles")
                )
                _options["cloudFiles.format"] = self.format

                if self.schema:
                    reader = reader.schema(self.schema)
                else:
                    schema_location = self.schema_location
                    if schema_location is None:
                        schema_location = os.path.dirname(self.path)
                    _options["cloudFiles.schemaLocation"] = schema_location

            _options["cloudFiles.inferColumnTypes"] = True
            _options["cloudFiles.schemaEvolutionMode"] = "addNewColumns"

        else:
            _mode = "static"
            reader = spark.read.format(self.format)

        # User Options
        _options["mergeSchema"] = True
        _options["recursiveFileLookup"] = True
        if self.read_options:
            for k, v in self.read_options.items():
                _options[k] = v

        reader = reader.options(**_options)

        # Load
        logger.info(f"Reading {self._id} as {_mode} and options {_options}")  # TODO: Add schema
        df = reader.load(self.path)

        return df

    def _read_polars(self) -> PolarsLazyFrame:

        import polars as pl

        if self.as_stream:
            raise ValueError(
                "Streaming read not supported with Pandas DataFrame. Please switch to Spark"
            )

        logger.info(f"Reading {self._id} as static")

        if self.format.lower() == "csv":
            df = pl.scan_csv(self.path, **self.read_options)

        elif self.format.lower() == "delta":
            df = pl.scan_delta(self.path, **self.read_options)

        elif self.format.lower() == "excel":
            df = pl.read_excel(self.path, **self.read_options)

        elif self.format.lower() == "json":
            if self.multiline:
                df = pl.scan_ndjson(self.path, **self.read_options)
            else:
                df = pl.read_json(self.path, **self.read_options)

        elif self.format.lower() == "parquet":
            df = pl.scan_parquet(self.path, **self.read_options)

        else:
            raise ValueError(f"Format '{self.format}' is not supported.")

        if isinstance(df, pl.DataFrame):
            df = df.lazy()

        return df
