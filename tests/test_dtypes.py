from __future__ import annotations

import narwhals as nw
import polars as pl
import pyspark.sql.types as T

from laktory.models import DType
from laktory.models import dtypes


def test_nw_supported():
    for name in dtypes.NAMES:
        assert hasattr(nw.dtypes, name)


def test_all_types():
    for name in dtypes.NAMES + list(dtypes.ALIASES.keys()):
        if name in ["Array", "List", "Struct"]:
            continue
        _ = DType(name=name)


def test_basic_types():
    t0 = DType(name="bigint")
    t1 = DType(name="FLOAT64")
    t2 = DType(name="str")

    # Narwhals
    assert t0.to_narwhals() == nw.Int64
    assert t1.to_narwhals() == nw.Float64
    assert t2.to_narwhals() == nw.String

    # Spark
    assert t0.to_spark() == T.LongType()
    assert t1.to_spark() == T.DoubleType()
    assert t2.to_spark() == T.StringType()

    # Polars
    assert t0.to_polars() == pl.Int64
    assert t1.to_polars() == pl.Float64
    assert t2.to_polars() == pl.String


def test_complex_types():
    t0 = DType(name="list", inner="int")
    t1 = DType(name="list", inner={"name": "list", "inner": "str"})
    t2 = dtypes.List(inner=DType(name="list", inner="str"))
    t3 = DType(name="struct", fields={"x": "double", "y": "int"})
    t4 = dtypes.Struct(fields={"x": {"name": "list", "inner": "double"}, "y": t3})

    # Narwhals
    assert t0.to_narwhals() == nw.List(inner=nw.Int32)
    assert t1.to_narwhals() == nw.List(inner=nw.List(inner=nw.String))
    assert t2.to_narwhals() == nw.List(inner=nw.List(inner=nw.String))
    assert t3.to_narwhals() == nw.Struct(
        fields=[
            nw.Field(name="x", dtype=nw.Float64),
            nw.Field(name="y", dtype=nw.Int32),
        ]
    )
    assert t4.to_narwhals() == nw.Struct(
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
    assert t0.to_spark() == T.ArrayType(T.IntegerType())
    assert t1.to_spark() == T.ArrayType(T.ArrayType(T.StringType()))
    assert t2.to_spark() == T.ArrayType(T.ArrayType(T.StringType()))
    assert t3.to_spark() == T.StructType(
        [T.StructField("x", T.DoubleType()), T.StructField("y", T.IntegerType())]
    )
    assert t4.to_spark() == T.StructType(
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
    assert t0.to_polars() == pl.List(pl.Int32)
    assert t1.to_polars() == pl.List(pl.List(pl.String))
    assert t2.to_polars() == pl.List(pl.List(pl.String))
    assert t3.to_polars() == pl.Struct(
        fields=[
            pl.Field(name="x", dtype=pl.Float64),
            pl.Field(name="y", dtype=pl.Int32),
        ]
    )
    assert t4.to_polars() == pl.Struct(
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


def test_explicit_types():
    assert DType(name="int32") == dtypes.Int32().to_generic()
    assert DType(name="double") == dtypes.Float64().to_generic()
    assert DType(name="str") == dtypes.String().to_generic()


def test_serialization():
    # List
    dtype0 = DType(name="list", inner=dtypes.String())
    dump0 = dtype0.model_dump(exclude_unset=True)
    dtype1 = DType.model_validate(dump0)
    dump1 = dtype1.model_dump(exclude_unset=True)
    assert dump1 == dump0

    # Struct
    dtype0 = DType(name="Struct", fields={"s": dtypes.String(), "x": dtypes.Float64()})
    dump0 = dtype0.model_dump(exclude_unset=True)
    dtype1 = DType.model_validate(dump0)
    dump1 = dtype1.model_dump(exclude_unset=True)
    assert dump1 == dump0

    # String
    dtype0 = dtypes.String()
    assert dtype0.model_dump(exclude_unset=True) == {"name": "String"}


if __name__ == "__main__":
    test_nw_supported()
    test_all_types()
    test_basic_types()
    test_complex_types()
    test_explicit_types()
    test_serialization()
