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
    name="slv_stock_prices",
    columns=[
        {
            "name": "open",
            "type": "double",
            "spark_func_name": "coalesce",
            "spark_func_args": ["data.open"],
        },
        {
            "name": "close",
            "type": "double",
            "sql_expression": "data.open"
        },
    ],
    data=[[1, 2], [3, 4], [5, 6]],
    zone="SILVER",
    catalog_name="dev",
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
    assert table.model_dump() == {
        "name": "slv_stock_prices",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": ["data.open"],
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
        "event_source": {
            "name": "stock_price",
            "description": None,
            "producer": None,
            "events_root": "/Volumes/dev/sources/landing/events/",
            "read_as_stream": True,
            "type": "STORAGE_EVENTS",
            "fmt": "JSON",
            "multiline": False,
        },
        "table_source": None,
        "zone": "SILVER",
        "pipeline_name": None,
    }

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")





if __name__ == "__main__":
    test_model()
    test_data()
