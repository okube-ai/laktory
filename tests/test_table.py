import pytest
from datetime import datetime
from pydantic import ValidationError
import pandas as pd

from laktory.models import Catalog
from laktory.models import Schema
from laktory.models import Column
from laktory.models import Table
from laktory.models import EventDataSource
from laktory.models import TableDataSource
from laktory._testing import table_brz
from laktory._testing import table_slv
from laktory._testing import EventsManager

manager = EventsManager()
manager.build_events()


def test_data():
    assert table_slv.df.equals(
        pd.DataFrame(
            {
                "open": [1, 3, 5],
                "close": [2, 4, 6],
            }
        )
    )


def test_model():
    print(table_slv.model_dump())
    assert table_slv.model_dump() == {
        "name": "slv_stock_prices",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "created_at",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "_created_at", "to_column": True, "to_lit": None}
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "timestamp",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "data.open", "to_column": True, "to_lit": None}
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "close",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [],
                "spark_func_kwargs": {},
                "spark_func_name": None,
                "sql_expression": "data.open",
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
        ],
        "primary_key": None,
        "comment": None,
        "catalog_name": "dev",
        "schema_name": "markets",
        "grants": None,
        "data": [[1, 2], [3, 4], [5, 6]],
        "timestamp_key": None,
        "event_source": None,
        "table_source": {
            "read_as_stream": True,
            "name": "brz_stock_prices",
            "schema_name": None,
            "catalog_name": None,
        },
        "zone": "SILVER",
        "pipeline_name": None,
    }

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")


def test_bronze():
    df0 = manager.to_spark_df()
    df1 = table_brz.process_bronze(df0)
    assert "_bronze_at" in df1.columns


def test_silver():
    df0 = manager.to_spark_df()
    df1 = table_brz.process_bronze(df0)
    df2 = table_slv.process_silver(df1)
    df2.show()
    # assert "_bronze_at" in df1.columns


if __name__ == "__main__":
    # test_model()
    # test_data()
    # test_bronze()
    test_silver()

