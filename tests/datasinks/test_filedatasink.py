import narwhals as nw
import pytest

from laktory._testing import get_df0
from laktory.enums import DataFrameBackends
from laktory.models.datasinks import FileDataSink
from laktory.models.datasources.filedatasource import SUPPORTED_FORMATS

from ..conftest import assert_dfs_equal

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

    # Test purge
    sink.purge()


def test_unknown_format():
    # Both backends accept unknown formats at construction; format validation happens at write time.
    # For PYSPARK, a warning is logged at write time and Spark handles the format natively.
    # For POLARS, _validate_format raises ValueError at write time (no generic writer fallback).
    sink = FileDataSink(path="tmp", format="LANCE", dataframe_backend="PYSPARK")
    assert sink.format == "LANCE"

    sink = FileDataSink(path="tmp", format="LANCE", dataframe_backend="POLARS")
    assert sink.format == "LANCE"


def test_purge_truncate_rejected(tmp_path):
    # Set on the sink: rejected for formats that can't be truncated
    for fmt in ["CSV", "PARQUET"]:
        with pytest.raises(ValueError, match="is not supported by FileDataSink"):
            FileDataSink(path=str(tmp_path / "sink"), reset_mode="TRUNCATE", format=fmt)


def test_purge_truncate_override_falls_back_to_drop(tmp_path):
    path = tmp_path / "sink"
    path.mkdir()
    (path / "data.csv").write_text("")
    sink = FileDataSink(path=str(path), format="CSV")
    sink.purge(mode="TRUNCATE")
    assert not path.exists()


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_purge_truncate(backend, tmp_path):
    """The rows are deleted, the table and its schema are kept"""
    fmt = "DELTA"
    df0 = get_df0(backend)
    path = (tmp_path / "sink").as_posix()
    mode = "OVERWRITE"

    sink = FileDataSink(format=fmt, path=path, mode=mode, reset_mode="TRUNCATE")
    sink.write(df0)
    columns = sink.as_source().read().columns

    sink.purge()

    df = sink.as_source().read()
    assert df.columns == columns
    if backend == "PYSPARK":
        assert df.to_native().count() == 0
    else:
        assert nw.from_native(df).lazy().collect().shape[0] == 0

    # Same table: truncated through a new version, history kept
    from deltalake import DeltaTable

    assert DeltaTable(path).version() == 1

    # Written again after the truncate
    sink.write(df0)
    assert_dfs_equal(sink.as_source().read(), df0)
