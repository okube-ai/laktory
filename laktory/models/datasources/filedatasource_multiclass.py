from __future__ import annotations

from pathlib import Path
from typing import Annotated
from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import BaseModel
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
    infer_schema:
        When `True`, the schema is inferred from the data. When `False`, the schema is
        not inferred and will be string if not specified in schema_definition. Only
        applicable to some format like CSV and JSON.
    read_options:
        Other options passed to dataframe backend reader.
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
    format: str
    path: str
    read_options: dict[str, Any] = {}
    type: Literal["FILE"] = Field("FILE", frozen=True)
    _reader: Any = None

    # CSV-specific
    has_header: bool = True
    infer_schema: bool = False
    schema_definition: DataFrameSchema = Field(None, validation_alias="schema")
    # schema_overrides: DataFrameSchema = Field(None, validation_alias="schema")
    schema_location: str = None

    # @field_validator("path", "schema_location", mode="before")
    @field_validator("path", mode="before")
    @classmethod
    def posixpath_to_string(cls, value: Any) -> Any:
        if isinstance(value, Path):
            value = str(value)
        return value

    @model_validator(mode="after")
    def validate_format(self) -> Any:
        if not self.reader.format_supported(self):
            raise ValueError(
                f"'{self.format}' format is not supported with {self.df_backend}. Use one of {self.reader.supported_formats}"
            )
        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def reader(self):
        print("SELECTED DF BACKEND", self.dataframe_backend, self.df_backend)

        if self._reader is None:
            if self.df_backend == DataFrameBackends.PYSPARK:
                if self.as_stream:
                    from laktory._readers.sparkstreamreader import (
                        SparkStreamReader as DataFrameReader,
                    )
                else:
                    from laktory._readers.sparkreader import (
                        SparkReader as DataFrameReader,
                    )
            elif self.df_backend == DataFrameBackends.POLARS:
                if self.as_stream:
                    raise ValueError()
                from laktory._readers.polarsreader import (
                    PolarsReader as DataFrameReader,
                )
            else:
                raise ValueError(f"{self.df_backend} is not supported")
            self._reader = DataFrameReader()
        return self._reader

    @property
    def _id(self):
        return str(self.path)

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _build_spark_kwargs(self):
        return {k: v for k, v in self.read_options}

    def _build_polars_kwargs(self):
        return {k: v for k, v in self.read_options}

    def _read_spark(self) -> nw.LazyFrame:
        kwargs = self._build_spark_kwargs()
        df = self.reader.read(fmt=self.format, path=self.path, **kwargs)
        return nw.from_native(df)

    def _read_polars(self) -> nw.LazyFrame:
        kwargs = self._build_polars_kwargs()
        df = self.reader.read(fmt=self.format, path=self.path, **kwargs)
        return nw.from_native(df)


class AvroDataSource(FileDataSource):
    format: Literal["AVRO"] = Field("AVRO", frozen=True)


class BinaryDataSource(FileDataSource):
    format: Literal["BINARY"] = Field("BINARY", frozen=True)


class CsvDataSource(FileDataSource):
    format: Literal["CSV"] = Field("CSV", frozen=True)
    has_header: bool = True
    infer_schema: bool = False
    schema_definition: DataFrameSchema = Field(None, validation_alias="schema")
    # schema_overrides: DataFrameSchema = Field(None, validation_alias="schema")
    schema_location: str = None

    def _build_spark_kwargs(self):
        kwargs = {}

        # Build Options
        if self.has_header is not None:
            kwargs["header"] = self.has_header
        if self.infer_schema is not None:
            kwargs["inferSchema"] = self.infer_schema
        if self.schema_definition is not None:
            kwargs["schema"] = self.schema_definition.to_spark()
        # TODO: Schema location

        for k, v in super()._build_spark_kwargs():
            kwargs[k] = v

        return kwargs

    def _build_polars_kwargs(self):
        kwargs = {}

        # Build Options
        if self.has_header is not None:
            kwargs["has_header"] = self.has_header
        if self.infer_schema is not None:
            kwargs["infer_schema"] = self.infer_schema
        if self.schema_definition is not None:
            kwargs["schema"] = self.schema_definition.to_polars()
        # TODO: Schema location

        for k, v in super()._build_spark_kwargs():
            kwargs[k] = v

        return kwargs


class DeltaDataSource(FileDataSource):
    format: Literal["DELTA"] = Field("DELTA", frozen=True)


class ExcelDataSource(FileDataSource):
    format: Literal["EXCEL"] = Field("EXCEL", frozen=True)


class IPCDataSource(FileDataSource):
    format: Literal["IPC"] = Field("IPC", frozen=True)


class JsonDataSource(FileDataSource):
    format: Literal["JSON", "JSONL", "NDJSON"] = Field("JSON", frozen=True)


class OrcDataSource(FileDataSource):
    format: Literal["ORC"] = Field("ORC", frozen=True)


class ParquetDataSource(FileDataSource):
    format: Literal["PARQUET"] = Field("PARQUET", frozen=True)


class PyArrowDataSource(FileDataSource):
    format: Literal["PYARROW"] = Field("PYARROW", frozen=True)


class TextDataSource(FileDataSource):
    format: Literal["TEXT"] = Field("TEXT", frozen=True)


class XmlDataSource(FileDataSource):
    format: Literal["XML"] = Field("XML", frozen=True)


AnyFileDataSource = Annotated[
    AvroDataSource
    | BinaryDataSource
    | CsvDataSource
    | DeltaDataSource
    | ExcelDataSource
    | IPCDataSource
    | JsonDataSource
    | OrcDataSource
    | ParquetDataSource
    | PyArrowDataSource
    | TextDataSource
    | XmlDataSource,
    Field(discriminator="format"),
]


def file_data_source_factory(fmt, backend):
    class SourceFactory(BaseModel):
        source: AnyFileDataSource = None

    s = SourceFactory(
        source={"format": fmt, "path": "/test/", "dataframe_backend": backend}
    )
    return type(s.source)
