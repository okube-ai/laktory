from pyspark.sql import Row
from pyspark.sql import SparkSession
import os
import pytest

from laktory.models.datasources import EventDataSource
from laktory._testing import StockPriceDataEventHeader
from laktory._testing import EventsManager


# Build and write events
data_dir = os.path.join(os.path.dirname(__file__), "data/")
header = StockPriceDataEventHeader()
manager = EventsManager()
manager.build_events(data_dir)
manager.to_path()

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()


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
        events_root=data_dir,
        read_as_stream=False,
    )
    df = source.read(spark).toPandas()
    assert len(df) == 80
    assert list(df.columns) == ["data", "description", "name", "producer"]
    df["data"] = df["data"].apply(Row.asDict)
    df["symbol"] = df["data"].apply(dict.get, args=("symbol",))
    df["created_at"] = df["data"].apply(dict.get, args=("_created_at",))
    df = df.sort_values(["symbol", "created_at"])
    row = df.iloc[0]["data"]
    assert row["symbol"] == "AAPL"
    assert row["close"] == pytest.approx(189.46, abs=0.01)


if __name__ == "__main__":
    test_event_data_source()
    test_event_data_source_read()
