from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()
    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"


if __name__ == "__main__":
    test_pipeline()
