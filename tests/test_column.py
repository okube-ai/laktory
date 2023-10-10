import os

from laktory.models import Column

GOOGL = {
    "name": "close",
    "type": "double",
    "unit": "USD",
    "catalog_name": "lakehouse",
    "schema_name": "markets",
    "table_name": "googl",
}

root_dir = os.path.dirname(__file__)


def test_model():
    c0 = Column(**GOOGL)
    c1 = Column.model_validate(GOOGL)
    assert c1.type == "double"
    assert c1.catalog_name == "lakehouse"
    assert c1.schema_name == "markets"
    assert c1.table_name == "googl"
    assert c1.full_name == "lakehouse.markets.googl.close"
    assert "func_name" in c1.model_fields
    assert c0 == c1


def test_read():
    c0 = Column(**GOOGL)

    with open(f"{root_dir}/googl.yaml", "r") as fp:
        c1 = Column.model_validate_yaml(fp)

    with open(f"{root_dir}/googl.json", "r") as fp:
        c2 = Column.model_validate_json_file(fp)

    assert c1 == c0
    assert c2 == c0


if __name__ == "__main__":
    test_model()
    test_read()
