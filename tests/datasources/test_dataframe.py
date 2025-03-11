import narwhals as nw
import polars as pl
import pytest

from laktory._testing import Paths
from laktory._testing import assert_dfs_equal
from laktory._testing import sparkf
from laktory.models.datasources import DataFrameDataSource

paths = Paths(__file__)
spark = sparkf.spark

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
    df = source.read(spark=spark)
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
    df = source.read(spark=spark)
    assert isinstance(df, nw.LazyFrame)
    assert_dfs_equal(df, df_target)


@pytest.mark.parametrize("df0", [dfs, dfp])
def test_drop_duplicates(df0):
    df1 = df0.union(df0)

    source = DataFrameDataSource(df=df1, drop_duplicates=True)
    df = source.read(spark=spark)

    assert_dfs_equal(df, df0)


# df1 = df0.union(df0)
#     assert df1.count() == df0.count() * 2
#
#     # Drop All
#     df = DataFrameDataSource(df=df1, drop_duplicates=True).read(spark)
#     assert df.to_native().count() == df0.count()
#
#     # Drop columns a and n
#     df = DataFrameDataSource(df=df1, drop_duplicates=["a", "n"]).read(spark)
#     assert df.to_native().count() == 2


def test_drops():
    pass


def test_filter():
    pass


def test_renames():
    pass


def test_sample():
    pass


def test_selects():
    pass


#
# def test_memory_data_source(df0=df0):
#     source = DataFrameDataSource(
#         df=df0,
#         filter="b != 0",
#         selects=["a", "b", "c"],
#         renames={"a": "aa", "b": "bb", "c": "cc"},
#         # broadcast=True,
#     )
#
#     # Test reader
#     df = source.read(spark)
#     assert df.columns == ["aa", "bb", "cc"]
#     assert df.to_native().count() == 2
#     # assert df.toPandas()["chain"].tolist() == ["chain", "chain"]
#
#     # Select with rename
#     source = DataFrameDataSource(
#         df=df0,
#         selects={"x": "x1", "n": "n1"},
#     )
#     df = source.read(spark)
#     assert df.columns == ["x1", "n1"]
#     assert df.to_native().count() == 3
#
#     # Drop
#     source = DataFrameDataSource(
#         df=df0,
#         drops=["b", "c", "n"],
#     )
#     df = source.read(spark)
#     assert df.columns == ["x", "a"]
#     assert df.to_native().count() == 3
#
#
# def test_drop_duplicates(df0=df0):
#     df1 = df0.union(df0)
#     assert df1.count() == df0.count() * 2
#
#     # Drop All
#     df = DataFrameDataSource(df=df1, drop_duplicates=True).read(spark)
#     assert df.to_native().count() == df0.count()
#
#     # Drop columns a and n
#     df = DataFrameDataSource(df=df1, drop_duplicates=["a", "n"]).read(spark)
#     assert df.to_native().count() == 2
#
#
# def test_memory_data_source_from_dict():
#     data0 = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}]
#     data1 = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
#     df_ref = pd.DataFrame(data0)
#
#     # Spark - data0
#     source = DataFrameDataSource(data=data0, dataframe_backend="PYSPARK")
#     df = source.read(spark)
#     assert df.to_native().toPandas().equals(df_ref)
#
#     # Spark - data1
#     source = DataFrameDataSource(data=data1, dataframe_backend="PYSPARK")
#     df = source.read(spark)
#     assert df.to_native().toPandas().equals(df_ref)
#
#     # Polars - data0
#     source = DataFrameDataSource(data=data0, dataframe_backend="POLARS")
#     df = source.read(spark)
#     assert df.collect().to_pandas().equals(df_ref)
#
#     # Polars - data1
#     source = DataFrameDataSource(data=data1, dataframe_backend="POLARS")
#     df = source.read(spark)
#     assert df.collect().to_pandas().equals(df_ref)
#
#
# def test_uc_data_source():
#     source = UnityCatalogDataSource(
#         catalog_name="dev",
#         schema_name="finance",
#         table_name="slv_stock_prices",
#     )
#
#     # Test meta
#     assert source.full_name == "dev.finance.slv_stock_prices"
#     assert source._id == "dev.finance.slv_stock_prices"
#
#     # TODO: Test read
#
#
# def test_source_selection():
#     class Model(BaseModel):
#         sources: list[
#             Union[FileDataSource, DataFrameDataSource, UnityCatalogDataSource]
#         ]
#
#     model = Model(
#         sources=[
#             {
#                 "path": "/some/path",
#             },
#             {
#                 "df": df0,
#             },
#             {
#                 "catalog_name": "dev",
#                 "schema_name": "finance",
#                 "table_name": "slv_stock_prices",
#             },
#         ]
#     )
#
#     source_types = [type(s) for s in model.sources]
#
#     assert source_types == [
#         FileDataSource,
#         DataFrameDataSource,
#         UnityCatalogDataSource,
#     ]
