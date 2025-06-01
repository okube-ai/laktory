import narwhals as nw
import polars as pl
import pytest

from laktory import get_spark_session
from laktory._testing import assert_dfs_equal
from laktory.models.datasources import DataFrameDataSource

spark = get_spark_session()

# DataFrame
data_dict = {
    "a": [1, -1, 1],
    "b": [2, 0, 2],
    "c": [3, 0, 3],
    "n": [4, 0, 4],
    "x": [1, 2, 3],
}
data_list = [dict(zip(data_dict.keys(), values)) for values in zip(*data_dict.values())]

dfs = spark.createDataFrame(data_list)
dfp = pl.DataFrame(data_dict).lazy()
df_target = nw.from_native(dfp)


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_read(df0):
    source = DataFrameDataSource(df=df0)
    df = source.read()
    assert isinstance(df, nw.LazyFrame)
    assert_dfs_equal(df, df_target)


@pytest.mark.parametrize(
    ["backend", "data"],
    [
        ("PYSPARK", data_dict),
        ("POLARS", data_dict),
        ("PYSPARK", data_list),
        ("POLARS", data_list),
    ],
)
def test_read_from_dict(backend, data):
    source = DataFrameDataSource(data=data, dataframe_backend=backend)
    df = source.read()
    assert isinstance(df, nw.LazyFrame)
    assert_dfs_equal(df, df_target)


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_drop_duplicates(df0):
    df1 = nw.concat([nw.from_native(df0), nw.from_native(df0)])

    source = DataFrameDataSource(df=df1, drop_duplicates=True)
    df = source.read()
    assert_dfs_equal(df, df0)

    source = DataFrameDataSource(df=df1, drop_duplicates=["a", "n"])
    df = source.read()
    assert_dfs_equal(df, df1.unique(["a", "n"]))


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_drops(df0):
    source = DataFrameDataSource(df=df0, drops=["a", "x"])
    df = source.read()
    assert df.columns == ["b", "c", "n"]


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_filter(df0):
    source = DataFrameDataSource(df=df0, filter="b != 0")
    df = source.read()
    assert_dfs_equal(df, nw.from_native(df0).filter(nw.col("b") != 0))


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_renames(df0):
    source = DataFrameDataSource(df=df0, renames={"a": "aa", "x": "col"})
    df = source.read()
    assert df.columns == ["aa", "b", "c", "n", "col"]
