from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()
    tables = pl.tables_dump()

    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"
    assert tables[0]["name"] == "brz_stock_prices"
    assert tables[0]["event_source"]["name"] == "stock_price"
    assert tables[0]["event_source"]["producer"]["name"] == "yahoo-finance"
    assert tables[0]["event_source"]["dirpath"] == "mnt/landing/events/yahoo-finance/stock_price/"
    assert tables[0]["event_source"]["read_as_stream"]
    assert tables[1]["name"] == "slv_stock_prices"
    assert tables[1]["table_source"]["name"] == "brz_stock_prices"


if __name__ == "__main__":
    test_pipeline()
