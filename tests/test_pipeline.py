import os
from datetime import datetime

from laktory.models import Catalog
from laktory.models import Table
from laktory.models import Pipeline
from laktory._testing import StockPricesPipeline

root_dir = os.path.dirname(__file__)


def test_pipeline():
    pl = StockPricesPipeline()

    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"


def test_read_yaml():
    with open(f"{root_dir}/pl-stocks.yaml", "r") as fp:
        pl = Pipeline.model_validate_yaml(fp)

    assert pl.name == "pl-stocks"


def test_tables_meta():
    pl = StockPricesPipeline()

    table = pl.get_tables_meta()
    df = table.df
    assert df["name"].tolist() == ["brz_stock_prices", "slv_stock_prices"]
    assert df["zone"].tolist() == ["BRONZE", "SILVER"]
    assert df["pipeline_name"].tolist() == ["pl-stock-prices", "pl-stock-prices"]
    assert df["comment"].tolist() == [None, None]


def test_columns_meta():
    pl = StockPricesPipeline()

    table = pl.get_columns_meta()
    df = table.df
    assert df["name"].tolist() == ["created_at", "low", "high"]
    assert df["type"].tolist() == ["timestamp", "double", "double"]


def test_publish_meta():
    # Timestamp is included in catalog name to prevent conflicts when running
    # multiple tests in parallel
    catalog_name = "laktory_testing_" + str(datetime.now().timestamp()).replace(".", "")

    pl = StockPricesPipeline()
    pl.publish_tables_meta(catalog_name=catalog_name)

    tables = Table(name="tables", schema_name="laktory", catalog_name=catalog_name)
    data = tables.select()
    assert data[0][0] == "brz_stock_prices"

    columns = Table(name="columns", schema_name="laktory", catalog_name=catalog_name)
    data = columns.select()
    assert data[1][0] == "low"

    # Cleanup
    Catalog(name=catalog_name).delete(force=True)


if __name__ == "__main__":
    test_pipeline()
    test_read_yaml()
    test_tables_meta()
    test_columns_meta()
    test_publish_meta()
