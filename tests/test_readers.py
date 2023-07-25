from laktory.readers import EventsReader

from laktory._testing.stockprice import StockPriceDefinition


def test_reader_event():
    reader = EventsReader(
        event=StockPriceDefinition(),
    )

    assert reader.load_path == "events/yahoo-finance/stock_price/"


if __name__ == "__main__":
    test_reader_event()
