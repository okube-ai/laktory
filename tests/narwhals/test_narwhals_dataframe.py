import narwhals as nw
import polars as pl
import pytest

import laktory  # noqa: F401
from laktory._testing import StreamingSource


@pytest.fixture()
def df0():
    return nw.from_native(
        pl.DataFrame(
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
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
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


def test_has_column(df0):
    assert df0.laktory.has_column("x@x")
    assert df0.laktory.has_column("`x@x`")
    assert df0.laktory.has_column("u[0].a")


def test_schema_flat(df0):
    schema = df0.laktory.schema_flat()
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


def test_union(df0):
    df = df0.laktory.union(df0)
    assert df.shape[0] == df.shape[0] * 2
    assert df.schema == df.schema


def test_signature(df0):
    sig = df0.select("x@x", "y", "z").laktory.signature()
    assert (
        sig
        == "DataFrame[x@x: Int64, y: List(String), z: Struct({'id': Int64, 'email': String})]"
    )


@pytest.mark.parametrize("is_lazy", [True, False])
def test_row_index(is_lazy):
    df0 = pl.DataFrame(
        {
            "x": ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
            "y": [1, 2, 3, 3, 2, 1, 1, 2, 3],
        }
    )

    if is_lazy:
        df0 = df0.lazy()

    df = nw.from_native(df0)

    if is_lazy:
        with pytest.raises(ValueError):
            df = df.laktory.with_row_index(name="i0")
    else:
        df = df.laktory.with_row_index(name="i0")
    df = df.laktory.with_row_index(name="i1", order_by="y")
    df = df.laktory.with_row_index(name="i2", partition_by="x", order_by="y")

    if is_lazy:
        df = df.collect()

    if not is_lazy:
        assert df.sort("i0")["i0"].to_list() == list(range(9))
    assert df.sort("y")["i1"].to_list() == list(range(9))
    assert df.sort("x", "y")["i2"].to_list() == [0, 1, 2] * 3


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

#
# if __name__ == "__main__":
#     import pandas as pd
#     import narwhals as nw
#
#     df_native = pd.DataFrame({
#         "a": ["x", "k", None, "d"],
#         "b": ["1", "2", None, "3"],
#         "c": [1, 2, None, 3],
#     })
#     df = nw.from_native(df_native)
#
#     df = df.with_columns(d=nw.lit(1))
#
#     df = df.with_columns(
#         nw.col("a").cum_count().alias("c0"),
#         nw.col("a").cum_count(reverse=True).alias("c1"),
#         nw.col("b").cum_count().alias("c2"),
#         nw.col("c").cum_count().alias("c3"),
#         nw.col("d").cum_count().alias("c4"),
#         # nw.lit(1).cum_count().alias("c3"),
#     )
#
#     print(df)
