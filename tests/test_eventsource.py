from laktory._testing import StockPriceSource


def test_eventsource():
    source = StockPriceSource()

    assert source.producer.name == "yahoo-finance"
    assert source.read_as_stream
    assert (
        source.event_root
        == "/Volumes/dev/sources/landing/events/yahoo-finance/stock_price/"
    )
    assert source.fmt == "JSON"
    assert not source.multiline


if __name__ == "__main__":
    test_eventsource()
