import narwhals as nw
import polars as pl
import pytest

import laktory  # noqa: F401
from laktory._testing import StreamingSource

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
df = nw.from_native(df)


# @pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
@pytest.mark.parametrize("backend", ["POLARS"])
def test_groupby_and_agg(backend):
    ss = StreamingSource()
    df0 = nw.concat(ss.get_dfs(3)).collect()

    df = df0.laktory.groupby_and_agg(
        groupby_columns=["_batch_id"],
        agg_expressions=[
            {
                # "name": "mean_close",
                "expr": "nw.col('x1').mean().alias('m0')",
            },
            "nw.col('_idx').mean().alias('m1')",
            nw.col("_idx").mean().alias("m2"),
        ],
    ).sort("_batch_id")
    assert df["m0"].to_list() == [2.0, 2.0, 2.0]
    assert df["m1"].to_list() == [1.0, 4.0, 7.0]
    assert df["m2"].to_list() == [1.0, 4.0, 7.0]


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
    assert df2.shape[0] == df.shape[0] * 2
    assert df2.schema == df.schema


#
# def test_window_filter():
#     df = dff.slv_polars.laktory.window_filter(
#         partition_by=["symbol"],
#         order_by=[
#             {"sql_expression": "created_at", "desc": True},
#         ],
#         drop_row_index=False,
#         rows_to_keep=2,
#     ).select("created_at", "symbol", "_row_index")
#
#     data = df.to_dict(as_series=False)
#     print(data)
#     assert data == {
#         "created_at": [
#             datetime.datetime(2023, 9, 28, 0, 0),
#             datetime.datetime(2023, 9, 29, 0, 0),
#             datetime.datetime(2023, 9, 28, 0, 0),
#             datetime.datetime(2023, 9, 29, 0, 0),
#             datetime.datetime(2023, 9, 28, 0, 0),
#             datetime.datetime(2023, 9, 29, 0, 0),
#             datetime.datetime(2023, 9, 28, 0, 0),
#             datetime.datetime(2023, 9, 29, 0, 0),
#         ],
#         "symbol": ["AAPL", "AAPL", "AMZN", "AMZN", "GOOGL", "GOOGL", "MSFT", "MSFT"],
#         "_row_index": [2, 1, 2, 1, 2, 1, 2, 1],
#     }
#
