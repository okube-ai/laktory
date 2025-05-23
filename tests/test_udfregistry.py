import narwhals as nw

from laktory import UDFRegistry
from laktory import register_udf
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0


def test_udf():
    df0 = get_df0("POLARS")

    @register_udf()
    def with_y1(df):
        return df.with_columns(y1=nw.col("x1"))

    @register_udf(name="y2")
    def with_y2(df, scale):
        return df.with_columns(y2=nw.col("x1") * scale)

    @register_udf(namespace="z")
    def with_y3(df):
        return df.with_columns(y3=nw.col("x1") * 3)

    udfs = UDFRegistry()

    df = udfs.get(name="with_y1")(df0)
    df = udfs.get(name="y2")(df, scale=10)
    df = udfs.get(namespace="z", name="with_y3")(df)

    df1 = with_y3(with_y2(with_y1(df0), scale=10))

    assert_dfs_equal(df, df1)
