from laktory._logger import get_logger
from laktory.models.dataframeschema import DataFrameSchema

logger = get_logger(__name__)

SUPPORTED_FORMATS = [
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
]


def read(
    format: str,
    path: str,
    *args,
    as_stream: bool = False,
    schema: DataFrameSchema = None,
    **kwargs,
):
    import polars as pl

    if as_stream:
        raise ValueError("Streaming not yet supported with Polars")

    if format == "AVRO":
        return pl.read_avro(path, *args, **kwargs).lazy()

    elif format == "CSV":
        return pl.scan_csv(path, *args, **kwargs)

    elif format == "DELTA":
        return pl.scan_delta(path, *args, **kwargs)

    elif format == "EXCEL":
        return pl.read_excel(path, *args, **kwargs).lazy()

    elif format == "IPC":
        return pl.scan_ipc(path, *args, **kwargs)

    elif format == "ICEBERG":
        return pl.scan_iceberg(path, *args, **kwargs)

    elif format == "JSON":
        if schema:
            kwargs["schema"] = schema.to_polars()
        return pl.read_json(path, *args, **kwargs).lazy()

    elif format in ["NDJSON", "JSONL"]:
        if schema:
            kwargs["schema"] = schema.to_polars()
        return pl.scan_ndjson(path, *args, **kwargs)

    elif format == "PARQUET":
        return pl.scan_parquet(path, *args, **kwargs)

    elif format == "PYARROW":
        return pl.scan_pyarrow_dataset(*args, **kwargs)
