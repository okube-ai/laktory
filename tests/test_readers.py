from laktory.readers import EventsReader

from laktory._testing.stockprice import StockPriceDefinition


def test_reader_event():
    reader = EventsReader(
        event_name="stock_price",
        producer_name="yahoo_finance",
    )
    assert reader.load_path == "mnt/landing/events/yahoo_finance/stock_price"


if __name__ == "__main__":
    test_reader_event()
