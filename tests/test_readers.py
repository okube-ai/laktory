from laktory.readers import EventReader

from laktory._testing.stockprice import StockPriceDefinition


def test_reader_event():
    reader = EventReader(
        event=StockPriceDefinition(),
    )

    assert reader.load_path == "events/yahoo-finance/stock_price/"


if __name__ == "__main__":
    test_reader_event()
