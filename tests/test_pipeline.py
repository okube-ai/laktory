from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()
    tables = pl.tables_dump()

    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"
    assert tables[0]["name"] == "brz_stock_prices"
    assert tables[0]["input_event"] == "yahoo-finance/stock_price"
    assert tables[0]["ingestion_pattern"]["read_as_stream"]
    assert tables[1]["name"] == "slv_stock_prices"
    assert tables[1]["input_table"] == "brz_stock_prices"


if __name__ == "__main__":
    test_pipeline()
