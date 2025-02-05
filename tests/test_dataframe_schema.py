import polars as pl
import pyspark.sql.types as T

from laktory.models import DataFrameSchema


def test_spark():
    s = DataFrameSchema(
        columns=[
            {"name": "x", "type": "bigint"},
            {"name": "y", "type": "FLOAT64"},
            {"name": "s", "type": "str"},
        ]
    )

    assert s.to_spark() == T.StructType(
        [
            T.StructField("x", T.LongType(), True),
            T.StructField("y", T.DoubleType(), True),
            T.StructField("s", T.StringType(), True),
        ]
    )


def test_polars():
    s = DataFrameSchema(
        columns=[
            {"name": "x", "type": "bigint"},
            {"name": "y", "type": "FLOAT64"},
            {"name": "s", "type": "str"},
        ]
    )

    assert s.to_polars() == pl.Schema(
        {
            "x": pl.Int64,
            "y": pl.Float64,
            "s": pl.String,
        }
    )


if __name__ == "__main__":
    test_spark()
    test_polars()
