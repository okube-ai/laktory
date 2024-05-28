import os
import pandas as pd

from laktory.models.datasources import FileDataSource
from laktory.models.datasources import MemoryDataSource
from laktory.models.datasources import TableDataSource
from laktory._testing import Paths
from laktory._testing import spark

paths = Paths(__file__)

# DataFrame
pdf = pd.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
    },
)
df0 = spark.createDataFrame(pdf)


def test_file_data_source():
    source = FileDataSource(
        path="Volumes/sources/landing/events/yahoo_finance/stock_price"
    )

    assert source.path == "Volumes/sources/landing/events/yahoo_finance/stock_price"
    assert source.dataframe_type == "SPARK"
    assert not source.as_stream
    assert not source.is_cdc


def test_file_data_source_read():
    source = FileDataSource(
        path=os.path.join(paths.data, "./events/yahoo-finance/stock_price"),
        as_stream=False,
    )
    df = source.read(spark)
    assert df.count() == 80
    assert df.columns == [
        "data",
        "description",
        "name",
        "producer",
    ]


def test_memory_data_source(df0=df0):
    source = MemoryDataSource(
        df=df0,
        filter="b != 0",
        selects=["a", "b", "c"],
        renames={"a": "aa", "b": "bb", "c": "cc"},
        broadcast=True,
    )

    # Test reader
    df = source.read(spark)
    assert df.columns == ["aa", "bb", "cc"]
    assert df.count() == 2
    # assert df.toPandas()["chain"].tolist() == ["chain", "chain"]

    # Select with rename
    source = MemoryDataSource(
        df=df0,
        selects={"x": "x1", "n": "n1"},
    )
    df = source.read(spark)
    assert df.columns == ["x1", "n1"]
    assert df.count() == 3

    # Drop
    source = MemoryDataSource(
        df=df0,
        drops=["b", "c", "n"],
    )
    df = source.read(spark)
    df.show()
    assert df.columns == ["x", "a"]
    assert df.count() == 3


def test_table_data_source():
    source = TableDataSource(
        catalog_name="dev",
        schema_name="finance",
        table_name="slv_stock_prices",
    )

    # Test meta
    assert source.full_name == "dev.finance.slv_stock_prices"
    assert source._id == "dev.finance.slv_stock_prices"


if __name__ == "__main__":
    test_file_data_source()
    test_file_data_source_read()
    test_memory_data_source()
    test_table_data_source()
