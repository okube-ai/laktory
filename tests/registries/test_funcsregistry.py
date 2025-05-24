import narwhals as nw

from laktory._registries.funcsregistry import FuncsRegistry
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.custom import func


def test_udf():
    df0 = get_df0("POLARS")

    @func()
    def with_y1(df):
        return df.with_columns(y1=nw.col("x1"))

    @func(name="y2")
    def with_y2(df, scale):
        return df.with_columns(y2=nw.col("x1") * scale)

    @func(namespace="z")
    def with_y3(df):
        return df.with_columns(y3=nw.col("x1") * 3)

    funcs = FuncsRegistry()

    df = funcs.get(name="with_y1")(df0)
    df = funcs.get(name="y2")(df, scale=10)
    df = funcs.get(namespace="z", name="with_y3")(df)

    df1 = with_y3(with_y2(with_y1(df0), scale=10))

    assert_dfs_equal(df, df1)
