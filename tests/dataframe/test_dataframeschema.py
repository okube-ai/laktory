import narwhals as nw
import polars as pl
import pyspark.sql.types as T
import pytest

from laktory.enums import DataFrameBackends
from laktory.models import DataFrameSchema
from laktory.models import DType
from laktory.models import dtypes

s = DataFrameSchema(
    columns=[
        {"name": "x", "dtype": "Int64"},
        {"name": "y", "dtype": "FLOAT64"},
        {"name": "s", "dtype": "string"},
        {"name": "vals", "dtype": {"name": "list", "inner": "string"}},
    ]
)


def test_dtype_round_trip():
    nw_dtype = nw.dtypes.Int64()
    dtype = DType.from_narwhals(nw_dtype)
    assert dtype.to_narwhals() == nw.dtypes.Int64()

    nw_dtype = nw.List(
        inner=nw.dtypes.Struct({"x": nw.dtypes.Float64(), "id": nw.dtypes.String()})
    )
    dtype = DType.from_narwhals(nw_dtype)
    assert dtype.to_narwhals() == nw_dtype


def test_validation():
    assert (
        DataFrameSchema(
            columns={
                "x": "int64",
                "y": {"dtype": "FLOAT64"},
                "s": dtypes.DType(name="string"),
                "vals": {"dtype": {"name": "list", "inner": "string"}},
            }
        )
        == s
    )


def test_narwhals():
    d = {
        "x": nw.dtypes.Int64,
        "y": nw.dtypes.Float64,
        "s": nw.dtypes.String,
        "vals": nw.List(nw.String),
    }
    assert s.to_narwhals() == nw.Schema(d)


def test_spark():
    assert s.to_pyspark() == T.StructType(
        [
            T.StructField("x", T.LongType(), True),
            T.StructField("y", T.DoubleType(), True),
            T.StructField("s", T.StringType(), True),
            T.StructField("vals", T.ArrayType(T.StringType()), True),
        ]
    )


def test_polars():
    assert s.to_polars() == pl.Schema(
        {"x": pl.Int64, "y": pl.Float64, "s": pl.String, "vals": pl.List(pl.String)}
    )


def test_native():
    assert s.to_native(DataFrameBackends.POLARS) == pl.Schema(
        {"x": pl.Int64, "y": pl.Float64, "s": pl.String, "vals": pl.List(pl.String)}
    )


def test_to_string():
    assert (
        s.to_string()
        == '{"x": "Int64", "y": "Float64", "s": "String", "vals": "List(String)"}'
    )


def test_from_narwhals():
    schema = DataFrameSchema.from_narwhals(s.to_narwhals())
    assert schema == s


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_from_df(backend):
    from laktory._testing import get_df0

    df = get_df0(backend)
    schema = DataFrameSchema.from_df(df)

    # Columns are populated from the narwhals schema
    assert [c.name for c in schema.columns] == ["_idx", "id", "x1"]

    # Native schema is cached
    assert schema._native_schema is not None

    # to_native() returns the cached object directly, bypassing column conversion
    assert schema.to_native() is schema._native_schema
