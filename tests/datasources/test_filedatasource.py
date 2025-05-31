from __future__ import annotations

import io

import narwhals as nw
import polars as pl
import pytest
from pydantic import ValidationError

import laktory
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameSchema
from laktory.models.datasources import FileDataSource
from laktory.models.datasources.filedatasource import SUPPORTED_FORMATS

pl_read_tests = [("POLARS", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.POLARS]]
spark_read_tests = [
    ("PYSPARK", fmt) for fmt in SUPPORTED_FORMATS[DataFrameBackends.PYSPARK]
]


@pytest.mark.parametrize(
    ["backend", "fmt"],
    pl_read_tests + spark_read_tests,
)
def test_read(backend, fmt, tmp_path):
    df0 = get_df0("POLARS").to_native()

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


@pytest.mark.parametrize(
    ["backend", "fmt"],
    [
        ("POLARS", "CSV"),
        ("POLARS", "JSON"),
        ("POLARS", "DELTA"),
        ("PYSPARK", "CSV"),
        ("PYSPARK", "JSON"),
        ("PYSPARK", "DELTA"),
    ],
)
def test_read_stream(backend, fmt, tmp_path):
    df0 = get_df0("POLARS").to_native()

    with pytest.raises(ValidationError):
        FileDataSource(
            path="./", format=fmt, dataframe_backend="POLARS", as_stream=True
        )

    filepath = tmp_path / f"df.{fmt}"
    if fmt == "CSV":
        df0.write_csv(filepath)
    elif fmt == "JSON":
        df0.write_json(filepath)
    elif fmt == "DELTA":
        df0.write_delta(tmp_path)
    else:
        raise ValueError(f"Format {fmt} not supported")

    source = FileDataSource(
        path=tmp_path,
        format=fmt,
        dataframe_backend="PYSPARK",
        as_stream=True,
    )

    if fmt == "CSV":
        assert source._get_spark_kwargs() == (
            {
                "header": True,
                "cloudFiles.inferColumnTypes": False,
                "cloudFiles.format": fmt.lower(),
                "recursiveFileLookup": True,
                "cloudFiles.schemaLocation": tmp_path.parent.as_posix(),
            },
            "cloudFiles",
        )
    elif fmt == "JSON":
        assert source._get_spark_kwargs() == (
            {
                "cloudFiles.inferColumnTypes": False,
                "cloudFiles.format": fmt.lower(),
                "recursiveFileLookup": True,
                "cloudFiles.schemaLocation": tmp_path.parent.as_posix(),
                "multiline": True,
            },
            "cloudFiles",
        )
    elif fmt == "DELTA":
        assert source._get_spark_kwargs() == ({}, "delta")

    if fmt in ["CSV", "JSON"]:
        pytest.skip("Requires Databricks Autoloader. Skipping Test.")

    df = source.read()
    assert df.schema == nw.Schema(
        {"_idx": nw.Int64(), "id": nw.String(), "x1": nw.Int64()}
    )
    assert df.to_native().isStreaming


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_csv_options(backend, tmp_path):
    df0 = get_df0("POLARS").to_native()

    csv = tmp_path / "df.csv"
    df0.write_csv(csv)

    # Default
    source = FileDataSource(path=csv, format="CSV", dataframe_backend=backend)
    assert source._get_polars_kwargs() == (
        {"has_header": True, "infer_schema": False},
        "csv",
    )
    assert source._get_spark_kwargs() == ({"header": True, "inferSchema": False}, "csv")
    df = source.read()
    assert_dfs_equal(
        df, df0.cast({"_idx": pl.String, "id": pl.String, "x1": pl.String})
    )

    # No Header
    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, has_header=False
    )
    assert source._get_polars_kwargs() == (
        {"has_header": False, "infer_schema": False},
        "csv",
    )
    assert source._get_spark_kwargs() == (
        {"header": False, "inferSchema": False},
        "csv",
    )
    df = source.read().collect(backend="polars")
    if backend == "PYSPARK":
        assert df.columns == ["_c0", "_c1", "_c2"]
        assert df.shape[0] == 4
    elif backend == "POLARS":
        assert df.columns == ["column_1", "column_2", "column_3"]
        assert df.shape[0] == 4

    # With Schema
    schema = DataFrameSchema(columns={"idx": "string", "xx": "string", "yy": "string"})
    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, schema=schema
    )
    df = source.read().collect(backend="polars")
    assert_dfs_equal(
        df,
        df0.cast({"_idx": pl.String, "id": pl.String, "x1": pl.String}).rename(
            {"_idx": "idx", "id": "xx", "x1": "yy"}
        ),
    )


def test_reader_methods(tmp_path):
    df0 = get_df0("POLARS").to_native()
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
def test_reader_kwargs(backend, tmp_path):
    df0 = get_df0("POLARS").to_native()
    print(df0)
    csv = tmp_path / "df.csv"
    df0.write_csv(csv)

    if backend == "PYSPARK":
        kwargs = {
            "header": False,
        }
    elif backend == "POLARS":
        kwargs = {"has_header": False, "new_columns": ["_c0", "_c1", "_c2"]}

    source = FileDataSource(
        path=csv, format="CSV", dataframe_backend=backend, reader_kwargs=kwargs
    )

    df = source.read()
    assert df.columns == ["_c0", "_c1", "_c2"]


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
