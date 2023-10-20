import pytest
from datetime import datetime
from pydantic import ValidationError
import pandas as pd

from laktory.models import Catalog
from laktory.models import Schema
from laktory.models import Column
from laktory.models import Table
from laktory.models import EventDataSource


table = Table(
    name="googl",
    columns=[
        {
            "name": "open",
            "type": "double",
        },
        {
            "name": "close",
            "type": "double",
        },
    ],
    data=[[1, 2], [3, 4], [5, 6]],
    zone="SILVER",
    catalog_name="lakehouse",
    schema_name="markets",
    event_source=EventDataSource(
        name="stock_price",
    ),
)


def test_data():
    assert table.df.equals(
        pd.DataFrame(
            {
                "open": [1, 3, 5],
                "close": [2, 4, 6],
            }
        )
    )


def test_model():
    assert table.columns == [
        Column(
            name="open",
            type="double",
            catalog_name="lakehouse",
            schema_name="markets",
            table_name="googl",
        ),
        Column(
            name="close",
            type="double",
            catalog_name="lakehouse",
            schema_name="markets",
            table_name="googl",
        ),
    ]
    assert table.catalog_name == "lakehouse"
    assert table.schema_name == "markets"
    assert table.parent_full_name == "lakehouse.markets"
    assert table.full_name == "lakehouse.markets.googl"
    assert table.zone == "SILVER"
    assert table.source.name == "stock_price"

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")


def test_sql_schema():
    types = Table.model_serialized_types()
    print(types)
    assert types == {
        "name": "string",
        "columns": [
            {
                "name": "string",
                "type": "string",
                "comment": "string",
                "catalog_name": "string",
                "schema_name": "string",
                "table_name": "string",
                "unit": "string",
                "pii": "boolean",
                "func_name": "string",
                "input_cols": ["string"],
                "func_kwargs": {},
                "jsonize": "boolean",
            }
        ],
        "primary_key": "string",
        "comment": "string",
        "catalog_name": "string",
        "schema_name": "string",
        "grants": [{"principal": "string", "privileges": ["string"]}],
        "data": [[None]],
        "timestamp_key": "string",
        "event_source": {
            "name": "string",
            "description": "string",
            "producer": {"name": "string", "description": "string", "party": "integer"},
            "events_root": "string",
            "read_as_stream": "boolean",
            "type": "string",
            "fmt": "string",
            "multiline": "boolean",
        },
        "table_source": {
            "read_as_stream": "boolean",
            "name": "string",
            "schema_name": "string",
            "catalog_name": "string",
        },
        "zone": "string",
        "pipeline_name": "string",
    }

    types = Column.model_serialized_types()
    assert types == {
        "name": "string",
        "type": "string",
        "comment": "string",
        "catalog_name": "string",
        "schema_name": "string",
        "table_name": "string",
        "unit": "string",
        "pii": "boolean",
        "func_name": "string",
        "input_cols": ["string"],
        "func_kwargs": {},
        "jsonize": "boolean",
    }

    types["func_kwargs"] = "string"
    schema = Column.model_sql_schema(types)
    assert (
        schema
        == "(name string, type string, comment string, catalog_name string, schema_name string, table_name string, unit string, pii boolean, func_name string, input_cols ARRAY<string>, func_kwargs string, jsonize boolean)"
    )


def atest_create_and_insert():
    # Timestamp is included in catalog name to prevent conflicts when running
    # multiple tests in parallel
    cat_name = "laktory_testing_" + str(datetime.now().timestamp()).replace(".", "")

    cat = Catalog(
        name=cat_name,
    )
    cat.create(if_not_exists=True)
    db = Schema(name="default", catalog_name=cat_name)
    db.create()
    table = Table(
        catalog_name=cat_name,
        schema_name="default",
        name="stocks",
        columns=[
            {
                "name": "open",
                "type": "double",
            },
            {
                "name": "close",
                "type": "double",
            },
            {
                "name": "symbol",
                "type": "string",
            },
            {
                "name": "d",
                "type": "string",
                "jsonize": True,
            },
        ],
        data=[
            [1, 2, "googl", {"a": 1}],
            [3, 4, "googl", {"b": 2}],
            [5, 6, "googl", {"c": 3}],
        ],
    )
    table.create(or_replace=True, insert_data=True)
    assert table.exists()
    data = table.select(load_json=True)
    assert data == [
        ["1.0", "2.0", "googl", {"a": 1}],
        ["3.0", "4.0", "googl", {"b": 2}],
        ["5.0", "6.0", "googl", {"c": 3}],
    ]
    table.delete()
    cat.delete(force=True)


def test_meta():
    meta = table.meta_table()

    meta.catalog_name = "main"

    assert "catalog_name" in meta.column_names
    assert "schema_name" in meta.column_names
    assert "name" in meta.column_names
    assert "comment" in meta.column_names
    assert "columns" in meta.column_names

    is_found = False
    for c in meta.columns:
        if c.name == "event_source":
            is_found = True
            assert (
                c.type
                == "STRUCT<name: string, description: string, producer: STRUCT<name: string, description: string, party: integer>, events_root: string, read_as_stream: boolean, type: string, fmt: string, multiline: boolean>"
            )
    assert is_found

    dump = meta.model_dump()
    col_dump = dump["columns"][0]
    assert list(col_dump.keys()) == [
        "name",
        "type",
        "comment",
        "catalog_name",
        "schema_name",
        "table_name",
        "unit",
        "pii",
        "func_name",
        "input_cols",
        "func_kwargs",
        "jsonize",
    ]


if __name__ == "__main__":
    test_model()
    test_data()
    test_sql_schema()
    # atest_create_and_insert()
    test_meta()
