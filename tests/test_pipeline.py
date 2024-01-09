from laktory._testing import StockPricesPipeline

pl = StockPricesPipeline()


def test_pipeline():
    print(pl.model_dump())
    assert pl.model_dump() == {
        "access_controls": [],
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
        "photon": None,
        "serverless": None,
        "storage": None,
        "tables": [
            {
                "builder": {
                    "aggregation": None,
                    "drop_columns": [],
                    "drop_duplicates": None,
                    "drop_source_columns": False,
                    "event_source": {
                        "name": "stock_price",
                        "description": None,
                        "producer": None,
                        "events_root_": None,
                        "read_as_stream": True,
                        "type": "STORAGE_EVENTS",
                        "fmt": "JSON",
                        "multiline": False,
                        "header": True,
                        "read_options": {},
                    },
                    "filter": None,
                    "joins": [],
                    "joins_post_aggregation": [],
                    "layer": "BRONZE",
                    "pipeline_name": "pl-stock-prices",
                    "selects": None,
                    "table_source": None,
                    "template": "BRONZE",
                    "window_filter": None,
                },
                "catalog_name": None,
                "columns": [],
                "comment": None,
                "data": None,
                "data_source_format": "DELTA",
                "expectations": [],
                "grants": None,
                "name": "brz_stock_prices",
                "primary_key": None,
                "schema_name": None,
                "table_type": "MANAGED",
                "timestamp_key": None,
                "view_definition": None,
                "warehouse_id": "08b717ce051a0261",
            },
            {
                "builder": {
                    "aggregation": None,
                    "drop_columns": [],
                    "drop_duplicates": None,
                    "drop_source_columns": True,
                    "event_source": None,
                    "filter": None,
                    "joins": [],
                    "joins_post_aggregation": [],
                    "layer": "SILVER",
                    "pipeline_name": "pl-stock-prices",
                    "selects": None,
                    "table_source": {
                        "read_as_stream": True,
                        "catalog_name": "dev",
                        "cdc": None,
                        "selects": None,
                        "filter": None,
                        "from_pipeline": True,
                        "name": "brz_stock_prices",
                        "schema_name": "markets",
                        "watermark": None,
                    },
                    "template": "SILVER",
                    "window_filter": None,
                },
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
                                "value": "data._created_at",
                                "is_column": True,
                                "to_lit": False,
                                "to_expr": True,
                            }
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
                            {
                                "value": "data.symbol",
                                "is_column": True,
                                "to_lit": False,
                                "to_expr": True,
                            }
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
                            {
                                "value": "data.open",
                                "is_column": True,
                                "to_lit": False,
                                "to_expr": True,
                            }
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
                    ["2023-11-01T00:00:00Z", "AAPL", 1, 2],
                    ["2023-11-01T01:00:00Z", "AAPL", 3, 4],
                    ["2023-11-01T00:00:00Z", "GOOGL", 3, 4],
                    ["2023-11-01T01:00:00Z", "GOOGL", 5, 6],
                ],
                "data_source_format": "DELTA",
                "expectations": [
                    {
                        "name": "positive_price",
                        "expression": "open > 0",
                        "action": "FAIL",
                    },
                    {
                        "name": "recent_price",
                        "expression": "created_at > '2023-01-01'",
                        "action": "DROP",
                    },
                ],
                "grants": None,
                "name": "slv_stock_prices",
                "primary_key": None,
                "schema_name": None,
                "table_type": "MANAGED",
                "timestamp_key": None,
                "view_definition": None,
                "warehouse_id": "08b717ce051a0261",
            },
        ],
        "target": None,
        "udfs": [{"module_name": "stock_functions", "function_name": "high"}],
    }


def test_pipeline_pulumi():
    assert pl.resource_name == "pl-stock-prices"
    assert pl.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
    }
    print(pl.pulumi_properties)
    assert pl.pulumi_properties == {
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "libraries": [],
        "name": "pl-stock-prices",
        "notifications": [],
    }

    # Resources
    assert len(pl.core_resources) == 3
    r = pl.core_resources[-1]
    r.options.aliases = ["my-file"]
    assert pl.core_resources[-1].options.aliases == ["my-file"]


if __name__ == "__main__":
    test_pipeline()
    test_pipeline_pulumi()
