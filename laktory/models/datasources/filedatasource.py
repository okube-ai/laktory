from typing import Union
import os.path
import json

from typing import Literal
from typing import Any
from pydantic import model_validator
from pydantic import Field
from pydantic import ConfigDict

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
    read_options:
        Other options passed to `spark.read.options`
    schema:
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

    format: Literal[
        "AVRO",
        "BINARYFILE",
        "CSV",
        "DELTA",
        "EXCEL",
        "JSON",
        "JSONL",
        "NDJSON",
        "ORC",
        "PARQUET",
        "TEXT",
        "XML",
    ] = "JSONL"
    path: str
    read_options: dict[str, Any] = {}
    schema_definition: Union[str, dict, list] = Field(None, validation_alias="schema")
    schema_location: str = None

    @model_validator(mode="after")
    def options(self) -> Any:

        if self.dataframe_backend == "SPARK":
            if self.format in [
                "EXCEL",
            ]:
                raise ValueError(f"'{self.format}' format is not supported with Spark")

        elif self.df_backend == "POLARS":
            if self.format in [
                "AVRO",
                "BINARYFILE",
                "ORC",
                "TEXT",
                "XML",
            ]:
                raise ValueError(f"'{self.format}' format is not supported with Polars")

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self.path)

    @property
    def _schema(self):
        schema = self.schema_definition
        if schema is None:
            return schema
        if isinstance(schema, list):
            schema = {"fields": schema, "type": "struct"}

        if isinstance(schema, str) and '"fields":[' in schema.replace("'", '"').replace(
            " ", ""
        ):
            schema = json.loads(schema)

        # DDL format
        if isinstance(schema, str):
            pass

        # Spark Struct format
        elif isinstance(schema, dict):
            import pyspark.sql.types as T

            schema = T.StructType.fromJson(schema)

        return schema

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:

        _options = {}
        _mode = "stream"

        # JSON
        _format = self.format
        if self.format in ["NDJSON", "JSONL"]:
            _format = "JSON"
            _options["multiline"] = False
        elif self.format == "JSON":
            _format = "JSON"
            _options["multiline"] = True

        # CSV
        if self.format == "CSV":
            _options["header"] = True

        if self.as_stream:
            _mode = "stream"

            if _format == "DELTA":
                reader = spark.readStream.format(_format.lower())

            else:
                reader = spark.readStream.format("cloudFiles")
                _options["cloudFiles.format"] = _format

                if self._schema:
                    reader = reader.schema(self._schema)
                else:
                    schema_location = self.schema_location
                    if schema_location is None:
                        schema_location = os.path.dirname(self.path)
                    _options["cloudFiles.schemaLocation"] = schema_location

            if self._schema is None:
                _options["cloudFiles.inferColumnTypes"] = True
                _options["cloudFiles.schemaEvolutionMode"] = "addNewColumns"

        else:
            _mode = "static"
            reader = spark.read.format(_format.lower())
            if self._schema:
                reader = reader.schema(self._schema)

        # User Options
        _options["mergeSchema"] = True
        _options["recursiveFileLookup"] = True
        if self.read_options:
            for k, v in self.read_options.items():
                _options[k] = v

        reader = reader.options(**_options)

        # Load
        logger.info(f"Reading {self._id} as {_mode} and options {_options}")
        if self._schema:
            schema_str = self._schema
            if hasattr(schema_str, "simpleString"):
                schema_str = schema_str.simpleString()
            else:
                schema_str = str(schema_str)
            logger.info(f"Expected schema: {schema_str}")
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
            df = pl.read_json(self.path, **self.read_options)

        elif self.format.lower() in ["jsonl", "ndjson"]:
            df = pl.scan_ndjson(self.path, **self.read_options)

        elif self.format.lower() == "parquet":
            df = pl.scan_parquet(self.path, **self.read_options)

        else:
            raise ValueError(f"Format '{self.format}' is not supported.")

        if isinstance(df, pl.DataFrame):
            df = df.lazy()

        return df
