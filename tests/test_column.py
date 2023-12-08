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
df = df.filter("data.symbol == 'GOOGL'").limit(5)


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

    c = Column(
        name="open",
        spark_func_args=[
            "data.open",
        ],
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'CAST(coalesce(data.open) AS STRING)'>"
    assert s == [
        "137.4600067138672",
        "135.44000244140625",
        "136.02000427246094",
        "133.58999633789062",
        "134.91000366210938",
    ]

    c = Column(
        name="open2",
        spark_func_name="poly1",
        spark_func_args=[
            "data.open",
        ],
        spark_func_kwargs={"a": {"value": 2.0, "to_lit": True}},
        type="double",
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'CAST(((2.0 * data.open) + 0.0) AS DOUBLE)'>"
    assert s == [
        274.9200134277344,
        270.8800048828125,
        272.0400085449219,
        267.17999267578125,
        269.82000732421875,
    ]

    c = Column(
        name="open_close",
        sql_expression="data.open*data.close",
        type="double",
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert (
        cs.__repr__()
        == "Column<'CAST((data.open * data.close) AS open_close AS DOUBLE)'>"
    )
    assert s == [
        18647.825014196802,
        18388.689710131846,
        18289.25068769534,
        18069.38217083132,
        18399.02695817873,
    ]

    c = Column(
        name="id",
        spark_func_name="coalesce",
        spark_func_args=[
            "data.`@id`",
        ],
        type="string",
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'CAST(coalesce(data.@id) AS STRING)'>"
    assert s == ["_id", "_id", "_id", "_id", "_id"]

    c = Column(
        name="no_explicit_type",
        spark_func_name="coalesce",
        spark_func_args=[
            "data.symbol",
        ],
        type="_any",
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'coalesce(data.symbol)'>"
    assert s == ["GOOGL", "GOOGL", "GOOGL", "GOOGL", "GOOGL"]

    c = Column(
        name="search_and_replace",
        spark_func_name="regexp_replace",
        spark_func_args=[
            "data.symbol",
            {"value": "O", "to_expr": False},
            {"value": "0", "to_expr": False},
        ],
        type="_any",
    )
    cs = c.to_spark(df)
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'regexp_replace(data.symbol, O, 0, 1)'>"
    assert s == ["G00GL", "G00GL", "G00GL", "G00GL", "G00GL"]


def test_spark_udf():
    def x2(x):
        return 2 * x

    def x_square(x):
        return x * x

    c = Column(
        name="m2",
        type="double",
        spark_func_name="x2",
        spark_func_args=[
            "data.open",
        ],
    )
    cs = c.to_spark(df, udfs=[x2, x_square])
    s = df.withColumn(c.name, cs).toPandas()[c.name].tolist()
    assert cs.__repr__() == "Column<'CAST((data.open * 2) AS DOUBLE)'>"
    assert s == [
        274.9200134277344,
        270.8800048828125,
        272.0400085449219,
        267.17999267578125,
        269.82000732421875,
    ]


if __name__ == "__main__":
    test_model()
    test_read()
    test_spark()
    test_spark_udf()
