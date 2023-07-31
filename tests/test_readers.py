from laktory.readers import EventsReader


def test_reader_event():
    reader = EventsReader(
        event_name="stock_price",
        producer_name="yahoo_finance",
    )
    assert reader.load_path == "mnt/landing/events/yahoo_finance/stock_price"

    load_path = "sources/provider/stocks"
    reader = EventsReader(
        load_path=load_path,
    )
    assert reader.load_path == load_path


if __name__ == "__main__":
    test_reader_event()
