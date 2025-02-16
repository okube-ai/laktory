import narwhals as nw
import polars as pl
import pyspark.sql.types as T

from laktory.models import DType


def test_basic_types():
    t0 = DType(name="bigint")
    t1 = DType(name="FLOAT64")
    t2 = DType(name="str")

    # Narwhals
    assert t0.to_nw == nw.Int64
    assert t1.to_nw == nw.Float64
    assert t2.to_nw == nw.String

    # Spark
    assert t0.to_spark == T.LongType()
    assert t1.to_spark == T.DoubleType()
    assert t2.to_spark == T.StringType()

    # Polars
    assert t0.to_polars == pl.Int64
    assert t1.to_polars == pl.Float64
    assert t2.to_polars == pl.String


def test_complex_types():
    t0 = DType(name="array", inner="int")
    t1 = DType(name="array", inner={"name": "array", "inner": "str"})
    t2 = DType(name="array", inner=DType(name="array", inner="str"))
    t3 = DType(name="struct", fields={"x": "double", "y": "int"})
    t4 = DType(
        name="struct", fields={"x": {"name": "array", "inner": "double"}, "y": t3}
    )

    # Narwhals
    assert t0.to_nw == nw.List(inner=nw.Int32)
    assert t1.to_nw == nw.List(inner=nw.List(inner=nw.String))
    assert t2.to_nw == nw.List(inner=nw.List(inner=nw.String))
    assert t3.to_nw == nw.Struct(
        fields=[
            nw.Field(name="x", dtype=nw.Float64),
            nw.Field(name="y", dtype=nw.Int32),
        ]
    )
    assert t4.to_nw == nw.Struct(
        fields=[
            nw.Field(name="x", dtype=nw.List(inner=nw.Float64)),
            nw.Field(
                name="y",
                dtype=nw.Struct(
                    fields=[
                        nw.Field(name="x", dtype=nw.Float64),
                        nw.Field(name="y", dtype=nw.Int32),
                    ]
                ),
            ),
        ]
    )

    # Spark
    assert t0.to_spark == T.ArrayType(T.IntegerType())
    assert t1.to_spark == T.ArrayType(T.ArrayType(T.StringType()))
    assert t2.to_spark == T.ArrayType(T.ArrayType(T.StringType()))
    assert t3.to_spark == T.StructType(
        [T.StructField("x", T.DoubleType()), T.StructField("y", T.IntegerType())]
    )
    assert t4.to_spark == T.StructType(
        [
            T.StructField("x", T.ArrayType(T.DoubleType())),
            T.StructField(
                "y",
                T.StructType(
                    [
                        T.StructField("x", T.DoubleType()),
                        T.StructField("y", T.IntegerType()),
                    ]
                ),
            ),
        ]
    )

    # Polars
    assert t0.to_polars == pl.List(pl.Int32)
    assert t1.to_polars == pl.List(pl.List(pl.String))
    assert t2.to_polars == pl.List(pl.List(pl.String))
    assert t3.to_polars == pl.Struct(
        fields=[
            pl.Field(name="x", dtype=pl.Float64),
            pl.Field(name="y", dtype=pl.Int32),
        ]
    )
    assert t4.to_polars == pl.Struct(
        fields=[
            pl.Field(name="x", dtype=pl.List(pl.Float64)),
            pl.Field(
                name="y",
                dtype=pl.Struct(
                    fields=[
                        pl.Field(name="x", dtype=pl.Float64),
                        pl.Field(name="y", dtype=pl.Int32),
                    ]
                ),
            ),
        ]
    )


if __name__ == "__main__":
    test_basic_types()
    test_complex_types()
