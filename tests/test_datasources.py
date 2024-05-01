from pyspark.sql import SparkSession
import os
import pandas as pd

from laktory.models.datasources import EventDataSource
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


def test_event_data_source():
    source = EventDataSource(
        name="stock_price",
        producer={"name": "yahoo_finance"},
    )
    assert (
        source.event_root
        == "/Volumes/dev/sources/landing/events/yahoo_finance/stock_price/"
    )
    assert not source.is_cdc


def test_event_data_source_read():
    source = EventDataSource(
        name="stock_price",
        producer={"name": "yahoo-finance"},
        events_root=os.path.join(paths.data, "./events/"),
        read_as_stream=False,
    )
    df = source.read(spark)
    assert df.count() == 80
    assert df.columns == [
        "data",
        "description",
        "event_root_",
        "name",
        "producer",
    ]


def test_table_data_source(df0=df0):
    source = TableDataSource(
        mock_df=df0,
        path="/Volumes/tables/stock_prices/",
        filter="b != 0",
        selects=["a", "b", "c"],
        renames={"a": "aa", "b": "bb", "c": "cc"},
    )

    # Test paths
    assert source.path == "/Volumes/tables/stock_prices/"
    assert source.from_path
    assert source.path_or_full_name == "/Volumes/tables/stock_prices/"

    # Test reader
    df = source.read(spark)
    assert df.columns == ["aa", "bb", "cc"]
    assert df.count() == 2

    # Select with rename
    source = TableDataSource(
        mock_df=df0,
        path="/Volumes/tables/stock_prices/",
        selects={"x": "x1", "n": "n1"},
    )
    df = source.read(spark)
    assert df.columns == ["x1", "n1"]
    assert df.count() == 3

    # Drop
    source = TableDataSource(
        mock_df=df0,
        path="/Volumes/tables/stock_prices/",
        drops=["b", "c", "n"],
    )
    df = source.read(spark)
    assert df.columns == ["x", "a"]
    assert df.count() == 3


if __name__ == "__main__":
    test_event_data_source()
    test_event_data_source_read()
    test_table_data_source()
