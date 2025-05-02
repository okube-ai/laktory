import narwhals as nw
import polars as pl

from laktory._testing import assert_dfs_equal
from laktory.narwhals import sql_expr


def test_sql_expr():
    df = pl.DataFrame(
        {
            "x": [1, 2, 6, 4],
            "y": [4, 5, 3, 4],
            "b1": [True, False, True, False],
            "b2": [True, True, False, False],
        }
    )
    df = nw.from_native(df)

    df = df.with_columns(o1=sql_expr("x+y"))
    df = df.select("o1", sql_expr("x+y").alias("o2"))

    assert_dfs_equal(df, pl.DataFrame({"o1": [5, 7, 9, 8], "o2": [5, 7, 9, 8]}))
