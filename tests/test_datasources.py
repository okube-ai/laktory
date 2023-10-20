from laktory.models.datasources import EventDataSource


def test_event_data_source():
    reader = EventDataSource(
        name="stock_price",
        producer={"name": "yahoo_finance"},
    )
    assert (
        reader.event_root
        == "/Volumes/dev/sources/landing/events/yahoo_finance/stock_price/"
    )


if __name__ == "__main__":
    test_event_data_source()
