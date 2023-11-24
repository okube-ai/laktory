import os
import json

import pandas as pd
import pytest
import yaml

from laktory.models import Column
from laktory._testing import EventsManager

GOOGL = {
    "name": "close",
    "type": "double",
    "unit": "USD",
    "catalog_name": "dev",
    "schema_name": "markets",
    "table_name": "stock_prices",
}
data_dir = os.path.join(os.path.dirname(__file__), "data/")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
with open(os.path.join(data_dir, "googl.json"), "w") as fp:
    json.dump(GOOGL, fp)
with open(os.path.join(data_dir, "googl.yaml"), "w") as fp:
    yaml.dump(GOOGL, fp)

# Events
manager = EventsManager()
manager.build_events()
df = manager.to_spark_df()


def test_model():
    c0 = Column(**GOOGL)
    c1 = Column.model_validate(GOOGL)
    assert c1.type == "double"
    assert c1.catalog_name == "dev"
    assert c1.schema_name == "markets"
    assert c1.table_name == "stock_prices"
    assert c1.full_name == "dev.markets.stock_prices.close"
    assert "spark_func_name" in c1.model_fields
    assert c0 == c1


def test_read():
    c0 = Column(**GOOGL)

    with open(f"{data_dir}/googl.yaml", "r") as fp:
        c1 = Column.model_validate_yaml(fp)

    with open(f"{data_dir}/googl.json", "r") as fp:
        c2 = Column.model_validate_json_file(fp)

    assert c1 == c0
    assert c2 == c0


def test_spark():
    with pytest.raises(ValueError):
        close = Column(name="x", spark_func_args=("x",)).to_spark(df)

    open = Column(
        name="open",
        spark_func_args=[
            "data.open",
        ],
    ).to_spark(df)
    assert open.__repr__() == "Column<'CAST(coalesce(data.open) AS STRING)'>"

    open2 = Column(
        name="open2",
        spark_func_name="poly1",
        spark_func_args=[
            "data.open",
        ],
        spark_func_kwargs={"a": {"value": 2.0, "to_lit": True}},
        type="double",
    ).to_spark(df)
    assert open2.__repr__() == "Column<'CAST(((2.0 * data.open) + 0.0) AS DOUBLE)'>"

    open_close = Column(
        name="open_close",
        sql_expression="open*close",
        type="double",
    ).to_spark(df)
    assert (
        open_close.__repr__()
        == "Column<'CAST((open * close) AS open_close AS DOUBLE)'>"
    )

    id = Column(
        name="id",
        spark_func_name="coalesce",
        spark_func_args=[
            "data.`@id`",
        ],
        type="string",
    ).to_spark(df)
    assert id.__repr__() == "Column<'CAST(coalesce(data.@id) AS STRING)'>"

    _ = Column(
        name="no_explicit_type",
        spark_func_name="coalesce",
        spark_func_args=[
            "data.symbol",
        ],
        type="_any",
    ).to_spark(df)
    assert _.__repr__() == "Column<'coalesce(data.symbol)'>"


def test_spark_udf():
    def x2(x):
        return 2 * x

    def x_square(x):
        return x * x

    m2 = Column(
        name="m2",
        spark_func_name="x2",
        spark_func_args=[
            "data.open",
        ],
    ).to_spark(df, udfs=[x2, x_square])
    assert m2.__repr__() == "Column<'CAST((data.open * 2) AS STRING)'>"


if __name__ == "__main__":
    test_model()
    test_read()
    test_spark()
    test_spark_udf()
