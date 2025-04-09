import io

import narwhals as nw
import polars as pl
import pytest
from pydantic import ValidationError

import laktory
from laktory._testing import assert_dfs_equal
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameSchema
from laktory.models.datasources import FileDataSource
from laktory.models.datasources.filedatasource import SUPPORTED_FORMATS

pl_read_tests = [("POLARS", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.POLARS]]
spark_read_tests = [
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
    pl_read_tests + spark_read_tests,
)
def test_read(backend, fmt, df0, tmp_path):
    filepath = tmp_path / f"df.{fmt}"

    kwargs = {}

    if fmt == "AVRO":
        df0.write_avro(filepath)
    elif fmt == "CSV":
        df0.write_csv(filepath)
        kwargs["infer_schema"] = True
    elif fmt == "EXCEL":
        pytest.skip("Missing library. Skipping Test.")
    elif fmt == "DELTA":
        filepath = tmp_path
        df0.write_delta(filepath)
    elif fmt == "JSON":
        df0.write_json(filepath)
    elif fmt in ["JSONL", "NDJSON"]:
        df0.write_ndjson(filepath)
    elif fmt == "IPC":
        df0.write_ipc(filepath)
    elif fmt == "PARQUET":
        df0.write_parquet(filepath)
    elif fmt == "PYARROW":
        import pyarrow.dataset as ds

        filepath = tmp_path
        ds.write_dataset(
            data=df0.to_arrow(), base_dir=filepath, format="parquet", partitioning=None
        )
    elif fmt == "TEXT":
        s = df0.write_json()
        with open(filepath, "w") as fp:
            fp.write(s)

    elif fmt == "XML":
        pytest.skip("Missing library. Skipping Test.")

    elif fmt == "ORC":
        spark = laktory.get_spark_session()
        _df = spark.createDataFrame(df0.to_pandas())
        _df.write.format(fmt).save(filepath.as_posix())

    elif fmt == "BINARYFILE":
        pytest.skip("Writing not supported for binary files. Skipping Test.")

    else:
        raise NotImplementedError()

    source = FileDataSource(
        format=fmt, path=filepath, dataframe_backend=backend, **kwargs
    )
    df = source.read()

    if fmt == "TEXT":
        _df = df.collect()
        df = pl.read_json(io.BytesIO(_df["value"][0].encode("utf-8")))

    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("format", ["CSV", "JSON", "DELTA"])
def test_read_stream(df0, format, tmp_path):
    with pytest.raises(ValidationError):
        FileDataSource(
            path="./", format=format, dataframe_backend="POLARS", as_stream=True
        )

    filepath = tmp_path / f"df.{format}"
    if format == "CSV":
        df0.write_csv(filepath)
    elif format == "JSON":
        df0.write_json(filepath)
    elif format == "DELTA":
        df0.write_delta(tmp_path)
    else:
        raise ValueError(f"Format {format} not supported")

    source = FileDataSource(
        path=tmp_path,
        format=format,
        dataframe_backend="PYSPARK",
        as_stream=True,
    )

    if format == "CSV":
        assert source._spark_kwargs == (
            {
                "header": True,
                "cloudFiles.inferColumnTypes": False,
                "cloudFiles.format": format.lower(),
                "recursiveFileLookup": True,
                "cloudFiles.schemaLocation": tmp_path.parent.as_posix(),
            },
            "cloudFiles",
        )
    elif format == "JSON":
        assert source._spark_kwargs == (
            {
                "cloudFiles.inferColumnTypes": False,
                "cloudFiles.format": format.lower(),
                "recursiveFileLookup": True,
                "cloudFiles.schemaLocation": tmp_path.parent.as_posix(),
                "multiline": True,
            },
            "cloudFiles",
        )
    elif format == "DELTA":
        assert source._spark_kwargs == ({}, "delta")

    if format in ["CSV", "JSON"]:
        pytest.skip("Requires Databricks Autoloader. Skipping Test.")

    df = source.read()
    assert df.schema == nw.Schema({"x": nw.String(), "y": nw.Int64()})
    assert df.to_native().isStreaming


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_csv_options(backend, df0, tmp_path):
    csv = tmp_path / "df.csv"
    df0.write_csv(csv)

    # Default
    source = FileDataSource(path=csv, format="CSV", dataframe_backend=backend)
    assert source._polars_kwargs == ({"has_header": True, "infer_schema": False}, "csv")
    assert source._spark_kwargs == ({"header": True, "inferSchema": False}, "csv")
    df = source.read()
    assert_dfs_equal(df, df0.cast({"x": pl.String, "y": pl.String}))

    # No Header
    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, has_header=False
    )
    assert source._polars_kwargs == (
        {"has_header": False, "infer_schema": False},
        "csv",
    )
    assert source._spark_kwargs == ({"header": False, "inferSchema": False}, "csv")
    df = source.read().collect(backend="polars")
    if backend == "PYSPARK":
        assert df.columns == ["_c0", "_c1"]
        assert df.shape[0] == 4
    elif backend == "POLARS":
        assert df.columns == ["column_1", "column_2"]
        assert df.shape[0] == 4

    # With Schema
    schema = DataFrameSchema(columns={"xx": "string", "yy": "string"})
    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, schema=schema
    )
    df = source.read().collect(backend="polars")
    assert_dfs_equal(
        df, df0.cast({"x": pl.String, "y": pl.String}).rename({"x": "xx", "y": "yy"})
    )


def test_reader_methods(df0, tmp_path):
    csv = tmp_path / "df.csv"
    df0.write_csv(csv)

    source = FileDataSource(
        path=csv,
        format="CSV",
        dataframe_backend="PYSPARK",
        reader_methods=[{"name": "schema", "args": ["x STRING, z FLOAT"]}],
    )
    df = source.read()
    assert df.columns == ["x", "z"]


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_reader_kwargs(backend, df0, tmp_path):
    csv = tmp_path / "df.csv"
    df0.write_csv(csv)

    if backend == "PYSPARK":
        kwargs = {
            "header": False,
        }
    elif backend == "POLARS":
        kwargs = {"has_header": False, "new_columns": ["_c0", "_c1"]}

    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, reader_kwargs=kwargs
    )

    df = source.read()
    assert df.columns == ["_c0", "_c1"]


def test_non_applicable_options():
    # has_header
    FileDataSource(path="tmp", format="CSV", has_header=True)
    with pytest.raises(ValidationError):
        FileDataSource(path="tmp", format="JSON", has_header=True)

    # infer_schema
    FileDataSource(path="tmp", format="PARQUET", infer_schema=True, as_stream=True)
    with pytest.raises(ValidationError):
        FileDataSource(path="tmp", format="PARQUET", infer_schema=True, as_stream=False)

    # schema_definition
    FileDataSource(
        path="tmp", format="JSON", schema={"columns": {}}, dataframe_backend="PYSPARK"
    )
    FileDataSource(
        path="tmp", format="JSON", schema={"columns": {}}, dataframe_backend="POLARS"
    )
    FileDataSource(
        path="tmp", format="DELTA", schema={"columns": {}}, dataframe_backend="PYSPARK"
    )
    with pytest.raises(ValidationError):
        FileDataSource(
            path="tmp",
            format="DELTA",
            schema={"columns": {}},
            dataframe_backend="POLARS",
        )
