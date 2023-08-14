import pytest
from pydantic import ValidationError
import pandas as pd

from laktory.models import Catalog
from laktory.models import Database
from laktory.models import Column
from laktory.models import Table


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
        database_name="markets",
    )


def test_data():
    assert table.df.equals(
        pd.DataFrame({
            "open": [1, 3, 5],
            "close": [2, 4, 6],
        })
    )


def test_model():
    assert table.columns == [
        Column(name="open", type="double", catalog_name="lakehouse", database_name="markets", table_name="googl"),
        Column(name="close", type="double", catalog_name="lakehouse", database_name="markets", table_name="googl"),
    ]
    assert table.catalog_name == "lakehouse"
    assert table.schema_name == "markets"
    assert table.parent_full_name == "lakehouse.markets"
    assert table.full_name == "lakehouse.markets.googl"
    assert table.zone == "SILVER"

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")


def test_create_and_insert():

    cat = Catalog(name="laktory_testing",)
    cat.create(if_not_exists=True)
    db = Database(name="default", catalog_name="laktory_testing")
    db.create()
    table = Table(
        catalog_name="laktory_testing",
        database_name="default",
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
            }
        ],
        data=[[1, 2, "googl"], [3, 4, "googl"], [5, 6, "googl"]],
    )
    table.create(or_replace=True, insert_data=True)
    assert table.exists()
    data = table.select()
    assert data == [['1.0', '2.0', 'googl'], ['3.0', '4.0', 'googl'], ['5.0', '6.0', 'googl']]
    table.delete(force=True)
    cat.delete(force=True)


def test_meta():
    meta = table.meta_table()
    meta.catalog_name = "main"

    assert "catalog_name" in meta.column_names
    assert "database_name" in meta.column_names
    assert "name" in meta.column_names
    assert "comment" in meta.column_names
    assert "columns" in meta.column_names


if __name__ == "__main__":
    test_model()
    test_data()
    test_create_and_insert()
    test_meta()
