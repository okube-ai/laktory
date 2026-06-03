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
    assert t0.to_pyspark() == T.LongType()
    assert t1.to_pyspark() == T.DoubleType()
    assert t2.to_pyspark() == T.StringType()

    # Polars
    assert t0.to_polars() == pl.Int64
    assert t1.to_polars() == pl.Float64
    assert t2.to_polars() == pl.String


def test_complex_types():
    t0 = DType(name="list", inner="int")
    t1 = DType(name="list", inner={"name": "list", "inner": "str"})
    t2 = dtypes.List(inner=DType(name="list", inner="str"))
    t3 = DType(
        name="struct",
        fields=[{"name": "x", "dtype": "double"}, {"name": "y", "dtype": "int"}],
    )
    t4 = dtypes.Struct(
        fields=[
            {"name": "x", "dtype": {"name": "list", "inner": "double"}},
            {"name": "y", "dtype": t3},
        ]
    )

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
    assert t0.to_pyspark() == T.ArrayType(T.IntegerType())
    assert t1.to_pyspark() == T.ArrayType(T.ArrayType(T.StringType()))
    assert t2.to_pyspark() == T.ArrayType(T.ArrayType(T.StringType()))
    assert t3.to_pyspark() == T.StructType(
        [T.StructField("x", T.DoubleType()), T.StructField("y", T.IntegerType())]
    )
    assert t4.to_pyspark() == T.StructType(
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


def test_datetime_types():
    # Default Datetime (time_zone="UTC") → TimestampType - B2 regression check
    t = DType(name="Datetime")
    assert t.time_zone == "UTC"
    assert t.to_pyspark() == T.TimestampType()
    assert t.to_narwhals() == nw.Datetime("us", "UTC")

    # Explicit time_zone → TimestampType
    t_tz = DType(name="Datetime", time_zone="America/New_York")
    assert t_tz.to_pyspark() == T.TimestampType()

    # Explicit time_zone=None → TimestampNTZType (TZ-naive)
    t_ntz = DType(name="Datetime", time_zone=None)
    assert t_ntz.to_pyspark() == T.TimestampNTZType()
    assert t_ntz.to_narwhals() == nw.Datetime("us")

    # "timestamp" alias maps to Datetime → TimestampType
    t_alias = DType(name="timestamp")
    assert t_alias.to_pyspark() == T.TimestampType()

    # time_unit propagates to narwhals
    t_ms = DType(name="Datetime", time_unit="ms")
    assert t_ms.to_narwhals() == nw.Datetime("ms", "UTC")

    # Duration
    t_dur = DType(name="Duration")
    assert t_dur.to_narwhals() == nw.Duration("us")
    t_dur_ms = DType(name="Duration", time_unit="ms")
    assert t_dur_ms.to_narwhals() == nw.Duration("ms")

    # from_narwhals round-trip preserves time_unit and time_zone
    nw_dt = nw.Datetime("ms", "UTC")
    rt = DType.from_narwhals(nw_dt)
    assert rt.name == "Datetime"
    assert rt.time_unit == "ms"
    assert rt.time_zone == "UTC"


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
    dtype0 = DType(
        name="Struct",
        fields=[
            {"name": "s", "dtype": dtypes.String()},
            {"name": "x", "dtype": dtypes.Float64()},
        ],
    )
    dump0 = dtype0.model_dump(exclude_unset=True)
    dtype1 = DType.model_validate(dump0)
    dump1 = dtype1.model_dump(exclude_unset=True)
    assert dump1 == dump0

    # String
    dtype0 = dtypes.String()
    assert dtype0.model_dump(exclude_unset=True) == {"name": "String"}


def test_dataframeschema_from_native():
    import pyspark.sql.types as T

    from laktory.models import DataFrameSchema

    native = T.StructType(
        [
            T.StructField("amount", T.DecimalType(10, 2)),
            T.StructField("name", T.StringType()),
        ]
    )
    import narwhals as nw

    nw_schema = nw.Schema({"amount": nw.Decimal(10, 2), "name": nw.String()})
    schema = DataFrameSchema.from_narwhals(nw_schema, native)

    # Columns are populated for introspection
    assert len(schema.columns) == 2
    assert schema.columns[0].name == "amount"

    # to_native() returns the original object directly (no re-conversion)
    assert schema.to_native() is native


def test_decimal_pyspark():
    import pyspark.sql.types as T

    from laktory import get_spark_session
    from laktory.models import DataFrameSchema

    # DType-level conversion
    dtype = DType(name="Decimal")
    pyspark_type = dtype.to_pyspark()
    assert isinstance(pyspark_type, T.DecimalType)

    # Schema-level conversion + createDataFrame (reproduces the reported error)
    schema = DataFrameSchema(columns={"amount": "Decimal", "name": "String"})
    pyspark_schema = schema.to_pyspark()
    assert isinstance(pyspark_schema, T.StructType)

    spark = get_spark_session()
    df = spark.createDataFrame(data=[], schema=pyspark_schema)
    assert df.count() == 0
