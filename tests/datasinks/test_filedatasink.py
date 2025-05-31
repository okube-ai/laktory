from __future__ import annotations

import narwhals as nw
import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.enums import DataFrameBackends
from laktory.models.datasinks import FileDataSink
from laktory.models.datasources.filedatasource import SUPPORTED_FORMATS

pl_write_tests = [
    ("POLARS", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.POLARS]
]
spark_write_tests = [
    ("PYSPARK", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.PYSPARK]
]


@pytest.mark.parametrize(
    ["backend", "fmt"],
    pl_write_tests + spark_write_tests,
)
def test_write(backend, fmt, tmp_path):
    df0 = get_df0(backend)

    kwargs = {}

    # Filepath
    filepath = tmp_path / f"df.{fmt}"

    # Format-specific configuration
    if fmt == "BINARYFILE":
        pytest.skip("Writing not supported for binary files. Skipping Test.")
    elif fmt == "XML":
        pytest.skip("Missing library. Skipping Test.")
    elif fmt == "TEXT":
        df0 = nw.from_native(df0).select(nw.col("id").alias("value")).to_native()
    elif fmt == "EXCEL":
        pytest.skip("Missing library. Skipping Test.")

    # Backend-specific configuration
    if backend == "PYSPARK":
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
        source = sink.as_source()
        if fmt.lower() in ["csv"]:
            source.has_header = True
            source.infer_schema = True

        df = source.read()

    elif backend == "POLARS":
        source = sink.as_source()
        if fmt.lower() in ["csv"]:
            source.has_header = True
            source.infer_schema = True

        df = source.read()

    else:
        raise ValueError(f"Backend {backend} is not configured")

    # Test
    assert_dfs_equal(df, df0)
