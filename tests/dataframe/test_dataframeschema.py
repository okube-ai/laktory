from __future__ import annotations

import narwhals as nw
import polars as pl
import pyspark.sql.types as T

from laktory.models import DataFrameSchema
from laktory.models import dtypes

s = DataFrameSchema(
    columns=[
        {"name": "x", "dtype": "Int64"},
        {"name": "y", "dtype": "FLOAT64"},
        {"name": "s", "dtype": "string"},
        {"name": "vals", "dtype": {"name": "list", "inner": "string"}},
    ]
)


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
    assert s.to_spark() == T.StructType(
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


def test_to_string():
    assert (
        s.to_string()
        == '{"x": "Int64", "y": "Float64", "s": "String", "vals": "List(String)"}'
    )
