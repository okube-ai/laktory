import narwhals as nw
import polars as pl
import pyspark.sql.types as T

from laktory.models import DataFrameSchema

s = DataFrameSchema(
    columns=[
        {"name": "x", "dtype": "bigint"},
        {"name": "y", "dtype": "FLOAT64"},
        {"name": "s", "dtype": "str"},
        {"name": "vals", "dtype": {"name": "list", "inner": "str"}},
    ]
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


if __name__ == "__main__":
    test_narwhals()
    test_spark()
    test_polars()
    test_to_string()
