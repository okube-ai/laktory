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
                "catalog_name": "dev",
                "comment": None,
                "name": "created_at",
                "pii": None,
                "raise_missing_arg_exception": True,
                "schema_name": "markets",
                "table_name": "slv_stock_prices",
                "type": "timestamp",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol",
                "pii": None,
                "raise_missing_arg_exception": True,
                "schema_name": "markets",
                "table_name": "slv_stock_prices",
                "type": "string",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "raise_missing_arg_exception": True,
                "schema_name": "markets",
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "close",
                "pii": None,
                "raise_missing_arg_exception": True,
                "schema_name": "markets",
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
        ],
        "comment": None,
        "data_source_format": "DELTA",
        "grants": None,
        "name": "slv_stock_prices",
        "properties": None,
        "primary_key": None,
        "schema_name": "markets",
        "storage_credential_name": None,
        "storage_location": None,
        "table_type": "MANAGED",
        "view_definition": None,
        "warehouse_id": "08b717ce051a0261",
    }


if __name__ == "__main__":
    test_model()
