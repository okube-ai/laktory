from pyspark.sql import SparkSession
from pyspark.sql import types as T

import laktory

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

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()

df = spark.createDataFrame(data, schema=schema)
df_brz = spark.read.parquet("./data/brz_stock_prices")
df_slv = spark.read.parquet("./data/slv_stock_prices")
df_meta = spark.read.parquet("./data/slv_stock_meta")


def test_df_schema_flat():
    df.show()
    df.printSchema()
    schema = df.schema_flat()
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
    assert df.has_column("x@x")
    assert df.has_column("`x@x`")
    assert df.has_column("u[0].a")


def test_watermark():
    df = df_slv.withWatermark("created_at", "1 hour")

    wm = df.watermark()

    assert wm["column"] == "created_at"
    assert wm["threshold"] == "1 hours"


def test_df_join():

    df_slv.laktory_join(
        other=df_meta
    )
    #
    # join = TableJoin(
    #     left={
    #         "name": "slv_stock_prices",
    #         "filter": "created_at = '2023-11-01T00:00:00Z'",
    #     },
    #     other={
    #         "name": "slv_stock_metadata",
    #         "selects": {
    #             "symbol2": "symbol",
    #             "currency": "currency",
    #             "first_traded": "first_traded",
    #         },
    #     },
    #     on=["symbol"],
    #     how="full_outer",
    # )
    # join.left._df = table_slv.to_df(spark)
    # join.other._df = df_meta
    #
    # df = join.execute(spark)
    #
    # # Test join columns uniqueness
    # _df = df.withColumn("symbol2", F.lit("a"))
    # # _df = df.withColumn("symbol", F.lit("a"))
    # _df = _df.select("symbol")
    #
    # # Test data
    # data = df.toPandas().fillna(-1).to_dict(orient="records")
    # print(data)
    # assert data == [
    #     {
    #         "created_at": "2023-11-01T00:00:00Z",
    #         "open": 1.0,
    #         "close": 2.0,
    #         "currency": "USD",
    #         "first_traded": "1980-12-12T14:30:00.000Z",
    #         "symbol": "AAPL",
    #     },
    #     {
    #         "created_at": -1,
    #         "open": -1.0,
    #         "close": -1.0,
    #         "currency": "USD",
    #         "first_traded": "1997-05-15T13:30:00.000Z",
    #         "symbol": "AMZN",
    #     },
    #     {
    #         "created_at": "2023-11-01T00:00:00Z",
    #         "open": 3.0,
    #         "close": 4.0,
    #         "currency": "USD",
    #         "first_traded": "2004-08-19T13:30:00.00Z",
    #         "symbol": "GOOGL",
    #     },
    # ]


if __name__ == "__main__":
    # test_df_schema_flat()
    # test_df_has_column()
    # test_df_join()

    test_watermark()

    # df = df_slv.withWatermark("created_at", "1 hour")
