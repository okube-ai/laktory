import pytest
from pydantic import ValidationError

from laktory.models.resources.databricks import Table
from laktory._testing.stockprices import spark


def test_model():

    table = Table(
        name="slv_stock_prices",
        columns=[
            {
                "name": "created_at",
                "type": "timestamp",
            },
            {
                "name": "symbol",
                "type": "string",
            },
            {
                "name": "open",
                "type": "double",
            },
            {
                "name": "close",
                "type": "double",
            },
        ],
        catalog_name="dev",
        schema_name="markets",
    )

    data = table.model_dump()
    print(data)
    assert data == {
        "catalog_name": "dev",
        "columns": [
            {
                "name": "created_at",
                "comment": None,
                "identity": None,
                "nullable": None,
                "type": "timestamp",
                "type_json": None,
            },
            {
                "name": "symbol",
                "comment": None,
                "identity": None,
                "nullable": None,
                "type": "string",
                "type_json": None,
            },
            {
                "name": "open",
                "comment": None,
                "identity": None,
                "nullable": None,
                "type": "double",
                "type_json": None,
            },
            {
                "name": "close",
                "comment": None,
                "identity": None,
                "nullable": None,
                "type": "double",
                "type_json": None,
            },
        ],
        "comment": None,
        "data_source_format": "DELTA",
        "grants": None,
        "name": "slv_stock_prices",
        "properties": None,
        "schema_name": "markets",
        "storage_credential_name": None,
        "storage_location": None,
        "table_type": "MANAGED",
        "view_definition": None,
        "warehouse_id": None,
    }


if __name__ == "__main__":
    test_model()
