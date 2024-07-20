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
    left = df_slv_polars.filter(
        pl.col("created_at") == datetime.datetime(2023, 9, 1)
    ).filter(pl.col("symbol") != "GOOGL")
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
    df2 = left.laktory.smart_join(
        other=df_meta_polars,
        left_on="symbol",
        other_on="symbol2",
    )
    assert "symbol" in df2.columns
    assert "symbol2" not in df2.columns
    assert df2.select(df.columns).equals(df)

    # Not Coalesce
    df4 = left.with_columns(open=pl.lit(None)).laktory.smart_join(
        other=df_meta_polars.with_columns(open=pl.lit(2)),
        left_on="symbol",
        other_on="symbol2",
    )
    assert df4.columns == [
        "created_at",
        "symbol",
        "open",
        "close",
        "currency",
        "first_traded",
        "open_right",
    ]

    # With Coalesce
    df5 = left.with_columns(open=pl.lit(None)).laktory.smart_join(
        other=df_meta_polars.with_columns(open=pl.lit(2)),
        left_on="symbol",
        other_on="symbol2",
        coalesce=True,
    )
    assert df5.columns == [
        "created_at",
        "symbol",
        "open",
        "close",
        "currency",
        "first_traded",
    ]
    assert df5["open"].fill_null(-1).to_list() == [2, 2, -1]


def test_join_outer():
    left = df_slv_polars.filter(
        pl.col("created_at") == datetime.datetime(2023, 9, 1)
    ).filter(pl.col("symbol") != "GOOGL")
    other = df_meta_polars.rename({"symbol2": "symbol"})

    df = left.laktory.smart_join(
        other=other,
        on=["symbol"],
        how="full",
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
            None,
            datetime.datetime(2023, 9, 1, 0, 0),
            datetime.datetime(2023, 9, 1, 0, 0),
        ],
        "symbol": ["AAPL", "GOOGL", "AMZN", "MSFT"],
        "open": [189.49000549316406, None, 139.4600067138672, 331.30999755859375],
        "close": [189.4600067138672, None, 138.1199951171875, 328.6600036621094],
        "currency": ["USD", "USD", "USD", None],
        "first_traded": [
            "1980-12-12T14:30:00.000Z",
            "2004-08-19T13:30:00.00Z",
            "1997-05-15T13:30:00.000Z",
            None,
        ],
    }


def test_aggregation():
    _df = df_slv_polars.filter(pl.col("created_at") < datetime.datetime(2023, 9, 7))

    # Symbol
    df = _df.laktory.groupby_and_agg(
        groupby_columns=["symbol"],
        agg_expressions=[
            {
                "name": "mean_close",
                "expr": "pl.col('close').mean()",
            },
        ],
    ).sort("symbol")
    assert "symbol" in df.columns
    assert df["mean_close"].round(2).to_list() == [187.36, 136.92, 135.3, 331.7]


def test_window_filter():
    df = df_slv_polars.laktory.window_filter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=2,
    ).select("created_at", "symbol", "_row_index")

    data = df.to_dict(as_series=False)
    print(data)
    assert data == {
        "created_at": [
            datetime.datetime(2023, 9, 28, 0, 0),
            datetime.datetime(2023, 9, 29, 0, 0),
            datetime.datetime(2023, 9, 28, 0, 0),
            datetime.datetime(2023, 9, 29, 0, 0),
            datetime.datetime(2023, 9, 28, 0, 0),
            datetime.datetime(2023, 9, 29, 0, 0),
            datetime.datetime(2023, 9, 28, 0, 0),
            datetime.datetime(2023, 9, 29, 0, 0),
        ],
        "symbol": ["AAPL", "AAPL", "AMZN", "AMZN", "GOOGL", "GOOGL", "MSFT", "MSFT"],
        "_row_index": [2, 1, 2, 1, 2, 1, 2, 1],
    }


if __name__ == "__main__":
    test_df_schema_flat()
    test_df_has_column()
    test_union()
    test_join()
    test_join_outer()
    test_aggregation()
    test_window_filter()
