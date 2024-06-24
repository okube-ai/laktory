import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pandas import Timestamp

import laktory
from laktory._testing.stockprices import spark
from laktory._testing.stockprices import df_slv
from laktory._testing.stockprices import df_meta

schema = T.StructType(
    [
        T.StructField("x@x", T.IntegerType()),
        T.StructField("y", T.ArrayType(T.StringType())),
        T.StructField(
            "z",
            T.StructType(
                [
                    T.StructField("id", T.IntegerType()),
                    T.StructField("email", T.StringType()),
                ]
            ),
        ),
        T.StructField(
            "u",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("a", T.IntegerType()),
                        T.StructField("b", T.IntegerType()),
                    ]
                )
            ),
        ),
        T.StructField(
            "u2",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("a", T.IntegerType()),
                        T.StructField("b", T.IntegerType()),
                    ]
                )
            ),
        ),
    ]
)

data = [
    (
        1,
        ["a", "b"],
        {"id": 3, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
    (
        2,
        ["b", "c"],
        {"id": 2, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
    (
        3,
        ["c", "d"],
        {"id": 1, "email": "@gmail.com"},
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
        [{"a": 1, "b": 2}, {"a": 1, "b": 2}],
    ),
]
df = spark.createDataFrame(data, schema=schema)

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_df_schema_flat():
    df.show()
    df.printSchema()
    schema = df.laktory.schema_flat()
    assert schema == [
        "x@x",
        "y",
        "z",
        "z.id",
        "z.email",
        "u",
        "u[*].a",
        "u[*].b",
        "u2",
        "u2[*].a",
        "u2[*].b",
    ]


def test_df_has_column():
    assert df.laktory.has_column("x@x")
    assert df.laktory.has_column("`x@x`")
    assert df.laktory.has_column("u[0].a")


def test_watermark():
    df = df_slv.withWatermark("created_at", "1 hour")

    wm = df.laktory.watermark()

    assert wm.column == "created_at"
    assert wm.threshold == "1 hours"

    # TODO: Add test for spark connect DataFrame


def test_join():
    left = df_slv.filter(F.col("created_at") == "2023-09-01T00:00:00Z").filter(
        F.col("symbol") != "GOOGL"
    )
    other = df_meta.withColumnRenamed("symbol2", "symbol")

    df = left.laktory.smart_join(
        other=other,
        on=["symbol"],
        how="left",
    )

    # Test join columns uniqueness
    _df = df.withColumn("symbol2", F.lit("a"))
    _df = df.withColumn("symbol", F.lit("a"))
    _df = _df.select("symbol")

    # Test data
    data = df.toPandas().fillna(-1).to_dict(orient="records")
    print(data)
    assert data == [
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 189.49000549316406,
            "close": 189.4600067138672,
            "currency": "USD",
            "first_traded": "1980-12-12T14:30:00.000Z",
            "symbol": "AAPL",
        },
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 139.4600067138672,
            "close": 138.1199951171875,
            "currency": "USD",
            "first_traded": "1997-05-15T13:30:00.000Z",
            "symbol": "AMZN",
        },
        # {
        #     "created_at": Timestamp("2023-09-01 00:00:00"),
        #     "open": 137.4600067138672,
        #     "close": 135.66000366210938,
        #     "currency": "USD",
        #     "first_traded": "2004-08-19T13:30:00.00Z",
        #     "symbol": "GOOGL",
        # },
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 331.30999755859375,
            "close": 328.6600036621094,
            "currency": -1,
            "first_traded": -1,
            "symbol": "MSFT",
        },
    ]

    # Join expression
    df2 = left.laktory.smart_join(
        other=df_meta.drop("symbol"),
        on_expression="left.symbol == other.symbol2",
    )
    assert "symbol" in df2.columns
    assert "symbol2" in df2.columns
    assert df2.select(df.columns).toPandas().equals(df.toPandas())

    # Left/Right expression
    df3 = left.laktory.smart_join(
        other=df_meta.drop("symbol"),
        left_on="symbol",
        other_on="symbol2",
    )
    assert "symbol" in df3.columns
    assert "symbol2" in df3.columns
    assert df3.toPandas().equals(df2.toPandas())

    # Not Coalesce
    df4 = left.withColumn("open", F.lit(None)).laktory.smart_join(
        other=df_meta.drop("symbol").withColumn("open", F.lit(2)),
        left_on="symbol",
        other_on="symbol2",
    )
    df4.printSchema()
    assert df4.columns == [
        "created_at",
        "symbol",
        "open",
        "close",
        "symbol2",
        "currency",
        "first_traded",
        "open",
    ]

    # With Coalesce
    df5 = left.withColumn("open", F.lit(None)).laktory.smart_join(
        other=df_meta.drop("symbol").withColumn("open", F.lit(2)),
        left_on="symbol",
        other_on="symbol2",
        coalesce=True,
    )
    assert df5.columns == [
        "created_at",
        "symbol",
        "close",
        "symbol2",
        "currency",
        "first_traded",
        "open",
    ]
    assert df5.toPandas()["open"].fillna(-1).to_list() == [2, 2, -1]


def test_join_outer():
    left = df_slv.filter(F.col("created_at") == "2023-09-01T00:00:00Z").filter(
        F.col("symbol") != "GOOGL"
    )
    other = df_meta.withColumnRenamed("symbol2", "symbol")

    df = left.laktory.smart_join(
        other=other,
        on=["symbol"],
        how="full_outer",
    )

    # Test join columns uniqueness
    _df = df.withColumn("symbol2", F.lit("a"))
    _df = df.withColumn("symbol", F.lit("a"))
    _df = _df.select("symbol")

    # Test data
    data = df.toPandas().fillna(-1).to_dict(orient="records")
    print(data)
    assert data == [
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 189.49000549316406,
            "close": 189.4600067138672,
            "currency": "USD",
            "first_traded": "1980-12-12T14:30:00.000Z",
            "symbol": "AAPL",
        },
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 139.4600067138672,
            "close": 138.1199951171875,
            "currency": "USD",
            "first_traded": "1997-05-15T13:30:00.000Z",
            "symbol": "AMZN",
        },
        {
            "created_at": -1,
            "open": -1,
            "close": -1,
            "currency": "USD",
            "first_traded": "2004-08-19T13:30:00.00Z",
            "symbol": "GOOGL",
        },
        {
            "created_at": Timestamp("2023-09-01 00:00:00"),
            "open": 331.30999755859375,
            "close": 328.6600036621094,
            "currency": -1,
            "first_traded": -1,
            "symbol": "MSFT",
        },
    ]


def test_join_watermark():
    # TODO
    pass


def test_aggregation():
    _df = df_slv.filter(F.col("created_at") < "2023-09-07T00:00:00Z")
    _df.show()

    # Window
    df = _df.laktory.groupby_and_agg(
        groupby_window={
            "time_column": "created_at",
            "window_duration": "1 day",
        },
        agg_expressions=[
            {
                "name": "min_open",
                "expr": "F.min('open')",
            },
            {
                "name": "max_open",
                "expr": "F.max('open')",
            },
        ],
    ).sort("window")
    pdf = df.toPandas()
    assert "window" in df.columns
    assert pdf["min_open"].round(2).to_list() == [137.46, 135.44, 136.02]
    assert pdf["max_open"].round(2).to_list() == [331.31, 329.0, 333.38]

    # Symbol
    df = _df.laktory.groupby_and_agg(
        groupby_columns=["symbol"],
        agg_expressions=[
            {
                "name": "mean_close",
                "expr": "F.mean('close')",
            },
        ],
    ).sort("symbol")
    pdf = df.toPandas()
    assert "symbol" in df.columns
    assert pdf["mean_close"].round(2).to_list() == [187.36, 136.92, 135.3, 331.7]

    # Symbol and window
    df = _df.laktory.groupby_and_agg(
        groupby_window={
            "time_column": "created_at",
            "window_duration": "1 day",
        },
        groupby_columns=["symbol"],
        agg_expressions=[
            {
                "name": "count",
                "expr": "F.count('close')",
            },
        ],
    ).sort("symbol", "window")
    pdf = df.toPandas()
    assert "symbol" in df.columns
    assert "window" in df.columns
    assert (
        pdf["count"].tolist()
        == [
            1,
        ]
        * _df.count()
    )


def test_window_filter():
    df = df_slv.laktory.window_filter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=2,
    ).select("created_at", "symbol", "_row_index")

    data = df.toPandas().to_dict(orient="records")
    assert data == [
        {
            "created_at": Timestamp("2023-09-29 00:00:00"),
            "symbol": "AAPL",
            "_row_index": 1,
        },
        {
            "created_at": Timestamp("2023-09-28 00:00:00"),
            "symbol": "AAPL",
            "_row_index": 2,
        },
        {
            "created_at": Timestamp("2023-09-29 00:00:00"),
            "symbol": "AMZN",
            "_row_index": 1,
        },
        {
            "created_at": Timestamp("2023-09-28 00:00:00"),
            "symbol": "AMZN",
            "_row_index": 2,
        },
        {
            "created_at": Timestamp("2023-09-29 00:00:00"),
            "symbol": "GOOGL",
            "_row_index": 1,
        },
        {
            "created_at": Timestamp("2023-09-28 00:00:00"),
            "symbol": "GOOGL",
            "_row_index": 2,
        },
        {
            "created_at": Timestamp("2023-09-29 00:00:00"),
            "symbol": "MSFT",
            "_row_index": 1,
        },
        {
            "created_at": Timestamp("2023-09-28 00:00:00"),
            "symbol": "MSFT",
            "_row_index": 2,
        },
    ]


if __name__ == "__main__":
    test_df_schema_flat()
    test_df_has_column()
    test_watermark()
    test_join()
    test_join_outer()
    test_join_watermark()
    test_aggregation()
    test_window_filter()
