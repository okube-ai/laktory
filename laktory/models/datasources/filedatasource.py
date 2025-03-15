from pathlib import Path
from typing import Any

import narwhals as nw
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.dataframeschema import DataFrameSchema
from laktory.models.datasources.basedatasource import BaseDataSource

logger = get_logger(__name__)


class FileDataSource(BaseDataSource):
    """
    Data source using disk files, such data events (json/csv) or full dataframes.
    Generally used in the context of a data pipeline.

    Attributes
    ----------
    format:
        Format of the data files
    read_options:
        Other options passed to `spark.read.options`
    schema_definition:
        Target schema specified as a list of columns, as a dict or a json
        serialization. Only used when reading data from non-strongly typed
        files such as JSON or csv files.
    schema_location:
        Path for schema inference when reading data as a stream. If `None`,
        parent directory of `path` is used.

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

    # With Explicit Schema
    source = models.FileDataSource(
        path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        format="JSON",
        as_stream=False,
        schema=[
            {"name": "description", "type": "string", "nullable": True},
            {"name": "close", "type": "double", "nullable": False},
        ],
    )
    # df = source.read(spark)
    ```
    """

    model_config = ConfigDict(populate_by_name=True)

    format: str = "JSONL"
    path: str
    read_options: dict[str, Any] = {}
    schema_definition: DataFrameSchema = Field(None, validation_alias="schema")
    schema_location: str = None
    type: str = Field("FILE", frozen=True)

    @field_validator("path", "schema_location", mode="before")
    @classmethod
    def posixpath_to_string(cls, value: Any) -> Any:
        if isinstance(value, Path):
            value = str(value)
        return value

    @model_validator(mode="after")
    def options(self) -> Any:
        with self.validate_assignment_disabled():
            self.format = self.format.upper()

        if self.dataframe_backend == DataFrameBackends.PYSPARK:
            from laktory.readers.sparkreader import SUPPORTED_FORMATS

            if self.format not in SUPPORTED_FORMATS:
                raise ValueError(
                    f"'{self.format}' format is not supported with Spark. Use one of {SUPPORTED_FORMATS}"
                )

        elif self.df_backend == DataFrameBackends.POLARS:
            from laktory.readers.polarsreader import SUPPORTED_FORMATS

            if self.format not in SUPPORTED_FORMATS:
                raise ValueError(
                    f"'{self.format}' format is not supported with Polars. Use one of {SUPPORTED_FORMATS}"
                )

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

    def _read_spark(self, spark=None) -> nw.LazyFrame:
        from laktory.readers.sparkreader import read

        df_spark = read(
            spark,
            fmt=self.format,
            path=self.path,
            as_stream=self.as_stream,
            schema=self.schema_definition,
            schema_location=self.schema_location,
        )

        return nw.from_native(df_spark)

    def _read_polars(self) -> nw.LazyFrame:
        from laktory.readers.polarsreader import read

        df_pl = read(
            fmt=self.format,
            path=self.path,
            as_stream=self.as_stream,
            schema=self.schema_definition,
        )

        return nw.from_native(df_pl)
