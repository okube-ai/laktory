from datetime import timedelta

import narwhals as nw
import polars as pl
import pytest

import laktory as lk  # noqa: F401
from laktory._testing import assert_dfs_equal
from laktory.datetime import utc_datetime


def test_current_timestamp():
    df0 = nw.from_native(pl.DataFrame({"x": [1, 2]}))

    t0 = utc_datetime()
    df = df0.with_columns(tstamp=nw.laktory.current_timestamp())

    assert "tstamp" in df.columns
    assert df.schema["tstamp"] == nw.Datetime()
    assert df.to_pandas()["tstamp"].max() - t0 < timedelta(seconds=1.0)


def test_sql_expr():
    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": [1, 2, 6, 4],
                "y": [4, 5, 3, 4],
                "b1": [True, False, True, False],
                "b2": [True, True, False, False],
            }
        )
    )

    df = df0.with_columns(o1=nw.laktory.sql_expr("x+y"))
    df = df.select("o1", nw.laktory.sql_expr("x+y").alias("o2"))

    assert_dfs_equal(df, pl.DataFrame({"o1": [5, 7, 9, 8], "o2": [5, 7, 9, 8]}))


@pytest.mark.xfail(reason="Not yet implemented")
def test_row_index():
    df = nw.from_native(
        pl.DataFrame(
            {
                "x": ["a", "a", "b", "b", "b", "c"],
                "z": ["11", "10", "22", "21", "20", "30"],
            }
        )
    )
    df = df.with_columns(y1=nw.Expr.laktory.row_number(nw.col("x")))
    df = df.with_columns(y2=nw.lit("x").cum_count())
    df = df.with_columns(y2=nw.Expr.laktory.row_number().over("x"))
    df = df.with_columns(y3=nw.Expr.laktory.row_number().over("x").sort_by("z"))
    assert df["y1"].to_list() == [1, 2, 3, 4, 5, 6]
    assert df["y2"].to_list() == [1, 2, 1, 2, 3, 1]
    assert df["y3"].to_list() == [2, 1, 3, 2, 1, 1]
