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
                "catalog_name": None,
                "columns": [],
                "comment": None,
                "data": None,
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
                "grants": None,
                "name": "brz_stock_prices",
                "pipeline_name": "pl-stock-prices",
                "primary_key": None,
                "schema_name": None,
                "table_source": None,
                "timestamp_key": None,
                "zone": "BRONZE",
            },
            {
                "catalog_name": None,
                "columns": [
                    {
                        "catalog_name": None,
                        "comment": None,
                        "name": "created_at",
                        "pii": None,
                        "schema_name": None,
                        "spark_func_args": [
                            {
                                "value": "_created_at",
                                "to_column": True,
                                "to_lit": False,
                            },
                            {
                                "value": "data._created_at",
                                "to_column": True,
                                "to_lit": False,
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
                            {"value": "data.symbol", "to_column": True, "to_lit": False}
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
                            {"value": "data.open", "to_column": True, "to_lit": False}
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
                "comment": None,
                "data": [
                    [None, "AAPL", 1, 2],
                    [None, "AAPL", 3, 4],
                    [None, "AAPL", 5, 6],
                ],
                "event_source": None,
                "grants": None,
                "name": "slv_stock_prices",
                "pipeline_name": "pl-stock-prices",
                "primary_key": None,
                "schema_name": None,
                "table_source": {
                    "read_as_stream": True,
                    "catalog_name": "dev",
                    "cdc": None,
                    "from_pipeline": True,
                    "name": "brz_stock_prices",
                    "schema_name": "markets",
                },
                "timestamp_key": None,
                "zone": "SILVER",
            },
        ],
        "target": None,
        "udfs": [{"module_name": "stock_functions", "function_name": "high"}],
    }


if __name__ == "__main__":
    test_pipeline()
