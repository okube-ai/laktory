import os

from laktory._logger import get_logger
from laktory.models.dataframeschema import DataFrameSchema

logger = get_logger(__name__)

SUPPORTED_FORMATS = [
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
]


def read(
    spark,
    fmt: str,
    path: str,
    *args,
    as_stream: bool = False,
    schema: DataFrameSchema = None,
    schema_location: str = None,
    **kwargs,
):
    _options = {}
    _mode = "static"

    # JSON
    _format = fmt.upper()
    if fmt.upper() in ["NDJSON", "JSONL"]:
        _format = "JSON"
        _options["multiline"] = False
    elif fmt.upper() == "JSON":
        _format = "JSON"
        _options["multiline"] = True

    # CSV
    if fmt.upper() == "CSV":
        _options["header"] = True

    if as_stream:
        _mode = "stream"

        if _format == "DELTA":
            reader = spark.readStream.format(_format.lower())

        else:
            reader = spark.readStream.format("cloudFiles")
            _options["cloudFiles.format"] = _format

            if schema:
                reader = reader.schema(schema.to_spark())
            else:
                if schema_location is None:
                    schema_location = os.path.dirname(path)
                _options["cloudFiles.schemaLocation"] = schema_location

        if schema is None:
            # _options["cloudFiles.inferColumnTypes"] = True # TODO: Review if we want to keep that default option
            _options["cloudFiles.schemaEvolutionMode"] = "addNewColumns"

    else:
        _mode = "static"
        reader = spark.read.format(_format.lower())
        if schema:
            reader = reader.schema(schema.to_spark())

    # User Options
    _options["mergeSchema"] = True
    _options["recursiveFileLookup"] = True
    if kwargs:
        for k, v in kwargs.items():
            _options[k] = v

    reader = reader.options(**_options)

    # Load
    logger.info(f"Reading {path} as {_mode} and options {_options}")
    if schema:
        logger.info(f"Expected schema: {schema.to_string()}")
    df = reader.load(path)

    return df
