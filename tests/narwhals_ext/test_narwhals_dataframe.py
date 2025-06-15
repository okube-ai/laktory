import narwhals as nw
import polars as pl
import pytest

import laktory as lk  # noqa: F401
from laktory._testing import StreamingSource
from laktory._testing import get_df0


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


@pytest.mark.parametrize(
    ("backend", "is_streaming"),
    [
        ("POLARS", False),
        ("PYSPARK", False),
        ("PYSPARK", True),
    ],
)
def test_dispaly(backend, is_streaming, tmp_path):
    if not is_streaming:
        df = get_df0(backend=backend)
    else:
        spark = lk.get_spark_session()

        ss = StreamingSource(backend)
        ss.write_to_delta(str(tmp_path))
        df = spark.readStream.format("DELTA").load(str(tmp_path))
        print(df.isStreaming)
        df = nw.from_native(df)

    # TODO: capture stdout to test output
    df.laktory.display(n=2, timeout=1)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_groupby_and_agg(backend):
    ss = StreamingSource(backend=backend)
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


def test_signature(df0):
    sig = df0.select("x@x", "y", "z").laktory.signature()
    assert (
        sig
        == "DataFrame[x@x: Int64, y: List(String), z: Struct({'id': Int64, 'email': String})]"
    )


def test_union(df0):
    df = df0.laktory.union(df0)
    assert df.shape[0] == df0.shape[0] * 2
    assert df.schema == df0.schema


def test_window_filter():
    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
                "y": [1, 2, 3, 3, 2, 1, 1, 2, 3],
            }
        )
    )

    df = df0.laktory.window_filter(
        partition_by=["x"],
        order_by="y",
        drop_row_index=False,
        rows_to_keep=2,
    )

    assert df.sort("x", "y")["_row_index"] == [0, 1] * 3
