import os
from pathlib import Path
from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import AliasChoices
from pydantic import ConfigDict
from pydantic import Field
from pydantic import computed_field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.dataframe.dataframeschema import DataFrameSchema
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.readerwritermethod import ReaderWriterMethod

logger = get_logger(__name__)

SUPPORTED_FORMATS = {
    DataFrameBackends.PYSPARK: [
        "AVRO",
        "BINARYFILE",
        "CSV",
        "DELTA",
        "JSON",
        "JSONL",
        "NDJSON",  # SAME AS JSONL
        "ORC",
        "PARQUET",
        "TEXT",
        "XML",
    ],
    DataFrameBackends.POLARS: [
        "AVRO",
        "CSV",
        "DELTA",
        "EXCEL",
        "IPC",
        # "ICEBERG", # TODO
        "JSON",
        "JSONL",
        "NDJSON",  # SAME AS JSONL
        "PARQUET",
        "PYARROW",
    ],
}

ALL_SUPPORTED_FORMATS = tuple(sorted(set().union(*SUPPORTED_FORMATS.values())))


class FileDataSource(BaseDataSource):
    """
    Data source using disk files, such data events (json/csv) or full dataframes.

    Examples
    ---------
    ```python
    from laktory import models

    source = models.FileDataSource(
        path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        format="JSON",
        dataframe_backend="POLARS",
    )
    # df = source.read()

    # With Explicit Schema
    source = models.FileDataSource(
        path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        format="JSON",
        dataframe_backend="PYSPARK",
        schema={
            "columns": {
                "symbol": "String",
                "open": "Float64",
                "close": "Float64",
            }
        },
    )
    # df = source.read()
    ```

    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    * [autoloader schema inference](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
    """

    model_config = ConfigDict(populate_by_name=True)
    format: Literal.__getitem__(ALL_SUPPORTED_FORMATS) = Field(
        ..., description="Format of the data files."
    )
    has_header: bool = Field(
        True,
        description="Indicate if the first row of the dataset is a header or not. Only applicable to 'CSV' format.",
    )
    infer_schema: bool = Field(
        False,
        description="""
        When `True`, the schema is inferred from the data. When `False`, the schema is not inferred and will be string 
        if not specified in schema_definition. Only applicable to some format like CSV and JSON.
        """,
    )
    path: str = Field(
        ...,
        description="File path on a local disk, remote storage or Databricks volume.",
    )
    reader_kwargs: dict[str, Any] = Field(
        {},
        description="""
        Keyword arguments passed directly to dataframe backend reader. Passed to `.options()` method when using PySpark.
        """,
    )
    schema_definition: DataFrameSchema = Field(
        None,
        validation_alias="schema",
        description="""
        Target schema specified as a list of columns, as a dict or a json serialization. Only used when
        reading data from non-strongly typed files such as JSON or csv files.
        """,
    )
    schema_location_: str | Path = Field(
        None,
        description="Path for schema inference when reading data as a stream. If `None`, parent directory of `path` is used.",
        validation_alias=AliasChoices("schema_location", "schema_location_"),
        exclude=True,
    )
    type: Literal["FILE"] = Field("FILE", frozen=True, description="Source Type")
    reader_methods: list[ReaderWriterMethod] = Field(
        [], description="DataFrame backend reader methods."
    )
    # schema_overrides: DataFrameSchema = Field(None, validation_alias="schema")

    @field_validator("path", mode="before")
    @classmethod
    def posixpath_to_string(cls, value: Any) -> Any:
        if isinstance(value, Path):
            value = str(value)
        return value

    @model_validator(mode="after")
    def validate_format(self) -> Any:
        if self.format not in SUPPORTED_FORMATS[self.dataframe_backend]:
            raise ValueError(
                f"'{self.format}' format is not supported with {self.dataframe_backend}. Use one of {SUPPORTED_FORMATS[self.dataframe_backend]}"
            )
        return self

    @model_validator(mode="after")
    def validate_options(self) -> Any:
        for k in [
            "has_header",
            "infer_schema",
            "schema_definition",
            "schema_location",
        ]:  # "schema_overrides",
            if k in self.model_fields_set:
                if not self._is_applicable(k):
                    raise ValueError(
                        f"Attribute `{k}` is not supported for the provider source format / backend / stream"
                    )
        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self.path)

    @computed_field(description="schema_location")
    @property
    def schema_location(self) -> Path:
        if self.schema_location_:
            return Path(self.schema_location_)

        return Path(os.path.dirname(self.path))

    @field_serializer("schema_location", when_used="json")
    def serialize_path(self, value: Path) -> str:
        return value.as_posix()

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _is_applicable(self, key):
        if key == "has_header":
            if self.format == "CSV":
                return True
            return False

        if key == "infer_schema":
            if self.format in ["CSV", "XML"]:
                return True

            if self.as_stream:
                # https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
                if self.format in [
                    "AVRO",
                    "CSV",
                    "JSON",
                    "JSONL",
                    "NDJSON",
                    "PARQUET",
                    "XML",
                ]:
                    return True

            return False

        if key == "schema_definition":
            if self.dataframe_backend == DataFrameBackends.PYSPARK:
                if self.format in [
                    "AVRO",
                    "CSV",
                    "DELTA",
                    "JSON",
                    "JSONL",
                    "NDJSON",
                    "ORC",
                    "PARQUET",
                ]:
                    return True

            if self.dataframe_backend == DataFrameBackends.POLARS:
                if self.format in ["CSV", "JSON", "JSONL", "NDJSON", "PARQUET"]:
                    return True

            return False

        if key == "schema_location":
            return self.is_cloud_files

        return False

    @property
    def is_cloud_files(self):
        return self.as_stream and self.format not in ["DELTA"]

    def _get_spark_kwargs(self):
        fmt = self.format.lower()

        # Build kwargs
        kwargs = {}

        if fmt in ["jsonl", "ndjson"]:
            kwargs["multiline"] = False
            fmt = "json"
        elif fmt in ["json"]:
            kwargs["multiline"] = True

        if self._is_applicable("has_header"):
            kwargs["header"] = self.has_header

        if self._is_applicable("infer_schema"):
            if self.as_stream:
                kwargs["cloudFiles.inferColumnTypes"] = self.infer_schema
            else:
                kwargs["inferSchema"] = self.infer_schema

        if self.as_stream:
            # Cloud Files Formats
            if self.is_cloud_files:
                kwargs["cloudFiles.format"] = fmt
                kwargs["recursiveFileLookup"] = True
                kwargs["cloudFiles.schemaLocation"] = self.schema_location.as_posix()
                fmt = "cloudFiles"

            # Native Streaming Formats
            else:
                pass

        for k, v in self.reader_kwargs.items():
            kwargs[k] = v

        return kwargs, fmt

    def _get_spark_reader_methods(self):
        methods = []

        options, fmt = self._get_spark_kwargs()

        if self.schema_definition:
            methods += [
                ReaderWriterMethod(
                    name="schema", args=[self.schema_definition.to_spark()]
                )
            ]

        methods += [ReaderWriterMethod(name="format", args=[fmt])]

        if options:
            methods += [ReaderWriterMethod(name="options", kwargs=options)]

        for m in self.reader_methods:
            methods += [m]

        return methods

    def _read_spark(self) -> nw.LazyFrame:
        from laktory import get_spark_session

        spark = get_spark_session()

        # Create reader
        if self.as_stream:
            mode = "stream"
            reader = spark.readStream
        else:
            mode = "static"
            reader = spark.read

        # Build methods
        methods = self._get_spark_reader_methods()

        # Apply methods
        for m in methods:
            reader = getattr(reader, m.name)(*m.args, **m.kwargs)

        # Load
        logger.info(
            f"Reading {self.path} as {mode} read.{'.'.join([m.as_string for m in methods])}"
        )
        df = reader.load(self.path)

        return nw.from_native(df)

    def _get_polars_kwargs(self):
        fmt = self.format.lower()

        kwargs = {}

        if self._is_applicable("has_header") and self.has_header is not None:
            kwargs["has_header"] = self.has_header

        if self._is_applicable("infer_schema") and self.infer_schema is not None:
            kwargs["infer_schema"] = self.infer_schema

        if (
            self._is_applicable("schema_definition")
            and self.schema_definition is not None
        ):
            kwargs["schema"] = self.schema_definition.to_polars()

        for k, v in self.reader_kwargs.items():
            kwargs[k] = v

        return kwargs, fmt

    def _read_polars(self) -> nw.LazyFrame:
        import polars as pl

        kwargs, fmt = self._get_polars_kwargs()

        logger.info(f"Reading {self.path} with format '{fmt}' and {kwargs}")

        if fmt == "avro":
            df = pl.read_avro(self.path, **kwargs).lazy()

        elif fmt == "csv":
            df = pl.scan_csv(self.path, **kwargs)

        elif fmt == "delta":
            df = pl.scan_delta(self.path, **kwargs)

        elif fmt == "excel":
            df = pl.read_excel(self.path, **kwargs).lazy()

        elif fmt == "ipc":
            df = pl.scan_ipc(self.path, **kwargs)

        elif fmt == "iceberg":
            df = pl.scan_iceberg(self.path, **kwargs)

        elif fmt == "json":
            df = pl.read_json(self.path, **kwargs).lazy()

        elif fmt in ["ndjson", "jsonl"]:
            df = pl.scan_ndjson(self.path, **kwargs)

        elif fmt == "parquet":
            df = pl.scan_parquet(self.path, **kwargs)

        elif fmt == "pyarrow":
            import pyarrow.dataset as ds

            dset = ds.dataset(self.path, format="parquet")
            return pl.scan_pyarrow_dataset(dset, **kwargs)

        else:
            raise ValueError(f"Format {fmt} is not supported.")

        return nw.from_native(df)
