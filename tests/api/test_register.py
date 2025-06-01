import narwhals as nw

import laktory as lk
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0


def test_register_df_namespace():
    df0 = get_df0("POLARS")

    @lk.api.register_anyframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_y1(self):
            return self._df.with_columns(y1=nw.col("x1"))

        def with_y2(self, scale):
            return self._df.with_columns(y2=nw.col("x1") * scale)

        def with_y3(self):
            return self._df.with_columns(y3=nw.col("x1") * 3)

    df = df0.custom.with_y1()
    df = df.custom.with_y2(scale=10)
    df = df.custom.with_y3()

    df1 = (
        df0.with_columns(y1=nw.col("x1"))
        .with_columns(y2=nw.col("x1") * 10)
        .with_columns(y3=nw.col("x1") * 3)
    )

    assert_dfs_equal(df, df1)


def test_register_expr_namespace():
    df0 = get_df0("POLARS")

    @lk.api.register_expr_namespace("custom")
    class CustomNamespace:
        def __init__(self, _expr):
            self._expr = _expr

        def mult2(self):
            return self._expr * 2

    df = df0.with_columns(m2=nw.col("x1").custom.mult2())

    df1 = df0.with_columns(m2=nw.col("x1") * 2)

    assert_dfs_equal(df, df1)
