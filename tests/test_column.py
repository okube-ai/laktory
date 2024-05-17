import os
import json
import yaml

from laktory.models.resources.databricks import Column
from laktory._testing import Paths
from laktory._testing.stockprices import df_brz as df

paths = Paths(__file__)
GOOGL = {
    "name": "close",
    "type": "double",
    "unit": "USD",
    "catalog_name": "dev",
    "schema_name": "markets",
    "table_name": "stock_prices",
}

with open(os.path.join(paths.tmp, "googl.json"), "w") as fp:
    json.dump(GOOGL, fp)
with open(os.path.join(paths.tmp, "googl.yaml"), "w") as fp:
    yaml.dump(GOOGL, fp)

# Spark
df = df.filter("data.symbol == 'GOOGL'").limit(5)


def test_model():
    c0 = Column(**GOOGL)
    c1 = Column.model_validate(GOOGL)
    assert c1.type == "double"
    assert c1.catalog_name == "dev"
    assert c1.schema_name == "markets"
    assert c1.table_name == "stock_prices"
    assert c1.full_name == "dev.markets.stock_prices.close"
    assert c0 == c1


def test_read():
    c0 = Column(**GOOGL)

    with open(f"{paths.tmp}/googl.yaml", "r") as fp:
        c1 = Column.model_validate_yaml(fp)

    with open(f"{paths.tmp}/googl.json", "r") as fp:
        c2 = Column.model_validate_json_file(fp)

    assert c1 == c0
    assert c2 == c0


if __name__ == "__main__":
    test_model()
    test_read()
