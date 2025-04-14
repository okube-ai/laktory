import polars as pl
import pytest

import laktory
from laktory._testing import assert_dfs_equal
from laktory.enums import DataFrameBackends
from laktory.models.datasinks import FileDataSink
from laktory.models.datasources.filedatasource import SUPPORTED_FORMATS

pl_write_tests = [
    ("POLARS", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.POLARS]
]
spark_write_tests = [
    ("PYSPARK", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.PYSPARK]
]


@pytest.fixture
def df0():
    return pl.DataFrame(
        {
            "x": ["a", "b", "c"],
            "y": [3, 4, 5],
        }
    )


@pytest.mark.parametrize(
    ["backend", "fmt"],
    pl_write_tests + spark_write_tests,
)
def test_write(backend, fmt, df0, tmp_path):
    kwargs = {}

    # Filepath
    filepath = tmp_path / f"df.{fmt}"

    # Format-specific configuration
    if fmt == "BINARYFILE":
        pytest.skip("Writing not supported for binary files. Skipping Test.")
    elif fmt == "XML":
        pytest.skip("Missing library. Skipping Test.")
    elif fmt == "TEXT":
        df0 = df0.select(pl.col("x").alias("value"))
    elif fmt == "EXCEL":
        pytest.skip("Missing library. Skipping Test.")

    # Backend-specific configuration
    if backend == "PYSPARK":
        spark = laktory.get_spark_session()
        df0 = spark.createDataFrame(df0.to_pandas())
        if fmt.lower() == "csv":
            kwargs["header"] = True
    elif backend == "POLARS":
        if fmt.lower() == "pyarrow":
            filepath = tmp_path

    # Set mode
    mode = None
    if backend == "PYSPARK" or fmt == "DELTA":
        mode = "OVERWRITE"

    # Create and read sinks
    sink = FileDataSink(
        format=fmt,
        path=filepath.as_posix(),
        mode=mode,
        writer_kwargs=kwargs,
    )
    sink.write(df0)

    # Read back DataFrame
    if backend == "PYSPARK":
        if fmt.lower() in ["jsonl", "ndjson"]:
            fmt = "JSON"
        df = (
            spark.read.format(fmt)
            .options(
                header=True,
                inferSchema=True,
            )
            .load(filepath.as_posix())
        )
    elif backend == "POLARS":
        if fmt.lower() == "avro":
            df = pl.read_avro(filepath)
        elif fmt.lower() == "csv":
            df = pl.read_csv(filepath)
        elif fmt.lower() == "delta":
            df = pl.read_delta(filepath.as_posix())
        elif fmt.lower() == "excel":
            df = pl.read_excel(filepath)
        elif fmt.lower() == "ipc":
            df = pl.read_ipc(filepath)
        elif fmt.lower() == "json":
            df = pl.read_json(filepath)
        elif fmt.lower() in ["jsonl", "ndjson"]:
            df = pl.read_ndjson(filepath)
        elif fmt.lower() == "parquet":
            df = pl.read_parquet(filepath)
        elif fmt.lower() == "pyarrow":
            import pyarrow.dataset as ds

            dset = ds.dataset(filepath.as_posix(), format="parquet")
            df = pl.scan_pyarrow_dataset(dset)
        else:
            raise ValueError(f"Format {fmt} is not configured")
    else:
        raise ValueError(f"Backend {backend} is not configured")

    # Test
    assert_dfs_equal(df, df0)
