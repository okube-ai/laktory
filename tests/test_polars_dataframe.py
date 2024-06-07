import datetime
import os
import polars as pl

import laktory
from laktory._testing.stockprices import df_slv_polars
from laktory._testing.stockprices import df_meta_polars

df = pl.DataFrame(
    [
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
    ],
    schema=["x@x", "y", "z", "u", "u2"],
)


def test_df_schema_flat():

    schema = df.laktory.schema_flat()
    print(df)
    print(df.schema)
    print(schema)

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


def test_union():
    df2 = df.laktory.union(df)

    assert df2.height == df.height * 2
    assert df2.schema == df.schema


def test_join():
    left = df_slv_polars.filter(pl.col("created_at") == datetime.datetime(2023, 9, 1)).filter(
        pl.col("symbol") != "GOOGL"
    )
    other = df_meta_polars.rename({"symbol2": "symbol"})

    df = left.laktory.smart_join(
        other=other,
        on=["symbol"],
        how="left",
    )

    # Test join columns uniqueness
    _df = df.with_columns(symbol2=pl.lit("a"))
    _df = df.with_columns(symbol=pl.lit("a"))
    _df = _df.select("symbol")

    # Test data
    data = df.to_dict(as_series=False)
    print(data)
    assert data == {
        "created_at": [
            datetime.datetime(2023, 9, 1, 0, 0),
            datetime.datetime(2023, 9, 1, 0, 0),
            datetime.datetime(2023, 9, 1, 0, 0),
        ],
        "symbol": ["AAPL", "AMZN", "MSFT"],
        "open": [189.49000549316406, 139.4600067138672, 331.30999755859375],
        "close": [189.4600067138672, 138.1199951171875, 328.6600036621094],
        "currency": ["USD", "USD", None],
        "first_traded": ["1980-12-12T14:30:00.000Z", "1997-05-15T13:30:00.000Z", None],
    }

    # Left and Other on
    print(left)
    print(df_meta_polars)
    df2 = left.laktory.smart_join(
        other=df_meta_polars.drop("symbol"),
        left_on="symbol",
        other_on="symbol2",
    )
    print(df2)
    assert "symbol" in df2.columns
    assert "symbol2" in df2.columns
    assert df2.equals(df)

#
# def test_join_outer():
#     left = df_slv.filter(F.col("created_at") == "2023-09-01T00:00:00Z").filter(
#         F.col("symbol") != "GOOGL"
#     )
#     other = df_meta.withColumnRenamed("symbol2", "symbol")
#
#     df = left.smart_join(
#         other=other,
#         on=["symbol"],
#         how="full_outer",
#     )
#
#     # Test join columns uniqueness
#     _df = df.withColumn("symbol2", F.lit("a"))
#     _df = df.withColumn("symbol", F.lit("a"))
#     _df = _df.select("symbol")
#
#     # Test data
#     data = df.toPandas().fillna(-1).to_dict(orient="records")
#     print(data)
#     assert data == [
#         {
#             "created_at": Timestamp("2023-09-01 00:00:00"),
#             "open": 189.49000549316406,
#             "close": 189.4600067138672,
#             "currency": "USD",
#             "first_traded": "1980-12-12T14:30:00.000Z",
#             "symbol": "AAPL",
#         },
#         {
#             "created_at": Timestamp("2023-09-01 00:00:00"),
#             "open": 139.4600067138672,
#             "close": 138.1199951171875,
#             "currency": "USD",
#             "first_traded": "1997-05-15T13:30:00.000Z",
#             "symbol": "AMZN",
#         },
#         {
#             "created_at": -1,
#             "open": -1,
#             "close": -1,
#             "currency": "USD",
#             "first_traded": "2004-08-19T13:30:00.00Z",
#             "symbol": "GOOGL",
#         },
#         {
#             "created_at": Timestamp("2023-09-01 00:00:00"),
#             "open": 331.30999755859375,
#             "close": 328.6600036621094,
#             "currency": -1,
#             "first_traded": -1,
#             "symbol": "MSFT",
#         },
#     ]
#
#
# def test_join_watermark():
#     # TODO
#     pass


def test_aggregation():
    _df = df_slv_polars.filter(pl.col("created_at") < datetime.datetime(2023, 9, 7))

    # Symbol
    df = _df.laktory.groupby_and_agg(
        groupby_columns=["symbol"],
        agg_expressions=[
            {
                "column": {"name": "mean_close"},
                "polars_func_name": "mean",
                "polars_func_args": ["close"],
            },
        ],
    ).sort("symbol")
    assert "symbol" in df.columns
    assert df["mean_close"].round(2).to_list() == [187.36, 136.92, 135.3, 331.7]


# def test_window_filter():
#     df = df_slv.window_filter(
#         partition_by=["symbol"],
#         order_by=[
#             {"sql_expression": "created_at", "desc": True},
#         ],
#         drop_row_index=False,
#         rows_to_keep=2,
#     ).select("created_at", "symbol", "_row_index")
#
#     data = df.toPandas().to_dict(orient="records")
#     assert data == [
#         {
#             "created_at": Timestamp("2023-09-29 00:00:00"),
#             "symbol": "AAPL",
#             "_row_index": 1,
#         },
#         {
#             "created_at": Timestamp("2023-09-28 00:00:00"),
#             "symbol": "AAPL",
#             "_row_index": 2,
#         },
#         {
#             "created_at": Timestamp("2023-09-29 00:00:00"),
#             "symbol": "AMZN",
#             "_row_index": 1,
#         },
#         {
#             "created_at": Timestamp("2023-09-28 00:00:00"),
#             "symbol": "AMZN",
#             "_row_index": 2,
#         },
#         {
#             "created_at": Timestamp("2023-09-29 00:00:00"),
#             "symbol": "GOOGL",
#             "_row_index": 1,
#         },
#         {
#             "created_at": Timestamp("2023-09-28 00:00:00"),
#             "symbol": "GOOGL",
#             "_row_index": 2,
#         },
#         {
#             "created_at": Timestamp("2023-09-29 00:00:00"),
#             "symbol": "MSFT",
#             "_row_index": 1,
#         },
#         {
#             "created_at": Timestamp("2023-09-28 00:00:00"),
#             "symbol": "MSFT",
#             "_row_index": 2,
#         },
#     ]

if __name__ == "__main__":
    # test_df_schema_flat()
    # test_df_has_column()
    # test_union()
    test_join()
    # test_join_outer()
    # test_join_watermark()
    # test_aggregation()
    # test_window_filter()
