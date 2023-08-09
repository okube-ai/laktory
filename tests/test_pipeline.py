from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()

    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"


def test_tables_meta():
    pl = StockPricesPipeline()

    table = pl.get_tables_meta()
    df = table.df
    assert df["name"].tolist() == ["brz_stock_prices", "slv_stock_prices"]
    assert df["zone"].tolist() == ["BRONZE", "SILVER"]
    assert df["pipeline_name"].tolist() == ["pl-stock-prices", "pl-stock-prices"]
    assert df["comment"].tolist() == [None, None]
    assert isinstance(df["columns"].iloc[-1], list)


def test_columns_meta():
    pl = StockPricesPipeline()

    table = pl.get_columns_meta()
    df = table.df
    assert df["name"].tolist() == ["created_at", "low", "high"]
    assert df["type"].tolist() == ["timestamp", "double", "double"]


if __name__ == "__main__":
    test_pipeline()
    test_tables_meta()
    test_columns_meta()
