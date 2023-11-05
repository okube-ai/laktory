from laktory._testing import StockPricesPipeline


def test_pipeline():
    pl = StockPricesPipeline()
    print(pl.model_dump())
    assert pl.model_dump() == {
        "allow_duplicate_names": None,
        "catalog": None,
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "continuous": None,
        "development": None,
        "edition": None,
        "libraries": [],
        "name": "pl-stock-prices",
        "notifications": [],
        "permissions": [],
        "photon": None,
        "serverless": None,
        "storage": None,
        "tables": [
            {
                "name": "brz_stock_prices",
                "columns": [],
                "primary_key": None,
                "comment": None,
                "catalog_name": None,
                "schema_name": None,
                "grants": None,
                "data": None,
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
                "zone": "BRONZE",
                "pipeline_name": "pl-stock-prices",
            },
            {
                "name": "slv_stock_prices",
                "columns": [
                    {
                        "catalog_name": None,
                        "comment": None,
                        "name": "created_at",
                        "pii": None,
                        "schema_name": None,
                        "spark_func_args": [
                            {"value": "_created_at", "to_column": True, "to_lit": None},
                            {
                                "value": "data._created_at",
                                "to_column": True,
                                "to_lit": None,
                            },
                        ],
                        "spark_func_kwargs": {},
                        "spark_func_name": "coalesce",
                        "sql_expression": None,
                        "table_name": "slv_stock_prices",
                        "type": "timestamp",
                        "unit": None,
                    },
                    {
                        "catalog_name": None,
                        "comment": None,
                        "name": "symbol",
                        "pii": None,
                        "schema_name": None,
                        "spark_func_args": [
                            {"value": "data.symbol", "to_column": True, "to_lit": None}
                        ],
                        "spark_func_kwargs": {},
                        "spark_func_name": "coalesce",
                        "sql_expression": None,
                        "table_name": "slv_stock_prices",
                        "type": "string",
                        "unit": None,
                    },
                    {
                        "catalog_name": None,
                        "comment": None,
                        "name": "open",
                        "pii": None,
                        "schema_name": None,
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
                        "catalog_name": None,
                        "comment": None,
                        "name": "close",
                        "pii": None,
                        "schema_name": None,
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
                "catalog_name": None,
                "schema_name": None,
                "grants": None,
                "data": [
                    [None, "AAPL", 1, 2],
                    [None, "AAPL", 3, 4],
                    [None, "AAPL", 5, 6],
                ],
                "timestamp_key": None,
                "event_source": None,
                "table_source": {
                    "read_as_stream": True,
                    "name": "brz_stock_prices",
                    "schema_name": None,
                    "catalog_name": None,
                },
                "zone": "SILVER",
                "pipeline_name": "pl-stock-prices",
            },
        ],
        "target": None,
    }


if __name__ == "__main__":
    test_pipeline()
