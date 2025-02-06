import narwhals as nw
import polars as pl
import pyspark.sql.types as T

from laktory.models import DataFrameSchema

s = DataFrameSchema(
    columns=[
        {"name": "x", "type": "bigint"},
        {"name": "y", "type": "FLOAT64"},
        {"name": "s", "type": "str"},
    ]
)


def test_narwhals():
    d = {
        "x": nw.dtypes.Int64,
        "y": nw.dtypes.Float64,
        "s": nw.dtypes.String,
    }
    assert s.to_narwhals() == nw.Schema(d)


def test_spark():
    assert s.to_spark() == T.StructType(
        [
            T.StructField("x", T.LongType(), True),
            T.StructField("y", T.DoubleType(), True),
            T.StructField("s", T.StringType(), True),
        ]
    )


def test_polars():
    assert s.to_polars() == pl.Schema(
        {
            "x": pl.Int64,
            "y": pl.Float64,
            "s": pl.String,
        }
    )


def test_to_string():
    assert s.to_string() == '{"x": "bigint", "y": "FLOAT64", "s": "str"}'


if __name__ == "__main__":
    test_narwhals()
    test_spark()
    test_polars()
    test_to_string()
