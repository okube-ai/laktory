from laktory.models.resources.databricks import Table


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
        table_type="MANAGED",
    )

    data = table.model_dump()
    print(data)
    assert data == {
        "catalog_name": "dev",
        "name": "slv_stock_prices",
        "schema_name": "markets",
        "table_type": "MANAGED",
        "cluster_id": None,
        "cluster_keys": None,
        "comment": None,
        "data_source_format": None,
        "options_": None,
        "owner": None,
        "partitions": None,
        "properties": None,
        "storage_credential_name": None,
        "storage_location": None,
        "view_definition": None,
        "warehouse_id": None,
        "column": [
            {
                "comment": None,
                "identity": None,
                "name": "created_at",
                "nullable": None,
                "type": "timestamp",
                "type_json": None,
            },
            {
                "comment": None,
                "identity": None,
                "name": "symbol",
                "nullable": None,
                "type": "string",
                "type_json": None,
            },
            {
                "comment": None,
                "identity": None,
                "name": "open",
                "nullable": None,
                "type": "double",
                "type_json": None,
            },
            {
                "comment": None,
                "identity": None,
                "name": "close",
                "nullable": None,
                "type": "double",
                "type_json": None,
            },
        ],
        "grant": None,
        "grants": None,
    }


if __name__ == "__main__":
    test_model()
