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
    fmt: str,
    path: str,
    *args,
    as_stream: bool = False,
    schema: DataFrameSchema = None,
    **kwargs,
):
    import polars as pl

    if as_stream:
        raise ValueError("Streaming not yet supported with Polars")

    if fmt.upper() == "AVRO":
        return pl.read_avro(path, *args, **kwargs).lazy()

    elif fmt.upper() == "CSV":
        return pl.scan_csv(path, *args, **kwargs)

    elif fmt.upper() == "DELTA":
        return pl.scan_delta(path, *args, **kwargs)

    elif fmt.upper() == "EXCEL":
        return pl.read_excel(path, *args, **kwargs).lazy()

    elif fmt.upper() == "IPC":
        return pl.scan_ipc(path, *args, **kwargs)

    elif fmt.upper() == "ICEBERG":
        return pl.scan_iceberg(path, *args, **kwargs)

    elif fmt.upper() == "JSON":
        if schema:
            kwargs["schema"] = schema.to_polars()
        return pl.read_json(path, *args, **kwargs).lazy()

    elif fmt.upper() in ["NDJSON", "JSONL"]:
        if schema:
            kwargs["schema"] = schema.to_polars()
        return pl.scan_ndjson(path, *args, **kwargs)

    elif fmt.upper() == "PARQUET":
        return pl.scan_parquet(path, *args, **kwargs)

    elif fmt.upper() == "PYARROW":
        import pyarrow.dataset as ds

        dset = ds.dataset(path, format="parquet")
        return pl.scan_pyarrow_dataset(dset, *args, **kwargs)

    else:
        raise ValueError(f"Format {fmt} is not supported.")
