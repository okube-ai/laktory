from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()
    tables = pl.tables_dump()
    columns = pl.columns_dump()

    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"
    assert tables[0]["name"] == "brz_stock_prices"
    assert tables[0]["event_source"]["name"] == "stock_price"
    assert tables[0]["event_source"]["producer"]["name"] == "yahoo-finance"
    assert tables[0]["event_source"]["dirpath"] == "mnt/landing/events/yahoo-finance/stock_price/"
    assert tables[0]["event_source"]["read_as_stream"]
    assert tables[1]["name"] == "slv_stock_prices"
    assert tables[1]["table_source"]["name"] == "brz_stock_prices"

    assert columns[0]['name'] == 'created_at'
    assert columns[0]['type'] == 'timestamp'
    assert columns[0]['func_name'] == 'coalesce'
    assert columns[0]['input_cols'] == ['_created_at']

    assert columns[1]['name'] == 'low'
    assert columns[1]['type'] == 'double'
    assert columns[1]['func_name'] == 'coalesce'
    assert columns[1]['input_cols'] == ['data.low']

    assert columns[2]['name'] == 'high'
    assert columns[2]['type'] == 'double'
    assert columns[2]['func_name'] == 'coalesce'
    assert columns[2]['input_cols'] == ['data.high']

    assert len(columns) == 3


if __name__ == "__main__":
    test_pipeline()
