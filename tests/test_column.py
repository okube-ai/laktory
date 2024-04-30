import os
import json
import yaml

from laktory.models import Column
from laktory._testing.stockprices import df_brz as df

GOOGL = {
    "name": "close",
    "type": "double",
    "unit": "USD",
    "catalog_name": "dev",
    "schema_name": "markets",
    "table_name": "stock_prices",
}
data_dir = os.path.join(os.path.dirname(__file__), "data_tmp/")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
with open(os.path.join(data_dir, "googl.json"), "w") as fp:
    json.dump(GOOGL, fp)
with open(os.path.join(data_dir, "googl.yaml"), "w") as fp:
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

    with open(f"{data_dir}/googl.yaml", "r") as fp:
        c1 = Column.model_validate_yaml(fp)

    with open(f"{data_dir}/googl.json", "r") as fp:
        c2 = Column.model_validate_json_file(fp)

    assert c1 == c0
    assert c2 == c0


if __name__ == "__main__":
    test_model()
    test_read()
