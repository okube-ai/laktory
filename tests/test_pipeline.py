from laktory import models

from laktory._testing.stockprices import table_slv_pl
from laktory._testing.stockprices import table_slv_join_pl


pl = models.Pipeline(
    name="pl-stock-prices",
    catalog="dev1",
    target="markets1",
    tables=[table_slv_pl, table_slv_join_pl],
    udfs=[
        {
            "module_name": "stock_functions",
            "function_name": "high",
        }
    ],
)


def test_pipeline():
    print(pl.model_dump())
    assert pl.model_dump() == {
        "access_controls": [],
        "allow_duplicate_names": None,
        "catalog": "dev1",
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "continuous": None,
        "development": None,
        "edition": None,
        "libraries": None,
        "name": "pl-stock-prices",
        "notifications": [],
        "photon": None,
        "serverless": None,
        "storage": None,
        "tables": [
            {
                "builder": {
                    "add_laktory_columns": True,
                    "as_dlt_view": False,
                    "drop_duplicates": None,
                    "drop_source_columns": True,
                    "event_source": None,
                    "layer": "SILVER",
                    "pipeline_name": "pl-stock-prices",
                    "table_source": {
                        "drops": None,
                        "filter": None,
                        "broadcast": False,
                        "read_as_stream": True,
                        "renames": None,
                        "selects": None,
                        "spark_chain": None,
                        "watermark": None,
                        "catalog_name": "dev",
                        "cdc": None,
                        "fmt": "DELTA",
                        "from_pipeline": True,
                        "name": "brz_stock_prices",
                        "path": None,
                        "schema_name": "markets",
                    },
                    "template": "SILVER",
                    "spark_chain": None,
                },
                "catalog_name": "dev1",
                "columns": [
                    {
                        "catalog_name": "dev1",
                        "comment": None,
                        "name": "created_at",
                        "pii": None,
                        "raise_missing_arg_exception": True,
                        "schema_name": "markets1",
                        "table_name": "slv_stock_prices",
                        "type": "timestamp",
                        "unit": None,
                    },
                    {
                        "catalog_name": "dev1",
                        "comment": None,
                        "name": "symbol",
                        "pii": None,
                        "raise_missing_arg_exception": True,
                        "schema_name": "markets1",
                        "table_name": "slv_stock_prices",
                        "type": "string",
                        "unit": None,
                    },
                    {
                        "catalog_name": "dev1",
                        "comment": None,
                        "name": "open",
                        "pii": None,
                        "raise_missing_arg_exception": True,
                        "schema_name": "markets1",
                        "table_name": "slv_stock_prices",
                        "type": "double",
                        "unit": None,
                    },
                    {
                        "catalog_name": "dev1",
                        "comment": None,
                        "name": "close",
                        "pii": None,
                        "raise_missing_arg_exception": True,
                        "schema_name": "markets1",
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
                "schema_name": "markets1",
                "table_type": "MANAGED",
                "timestamp_key": None,
                "view_definition": None,
                "warehouse_id": "08b717ce051a0261",
            },
            {
                "builder": {
                    "add_laktory_columns": True,
                    "as_dlt_view": False,
                    "drop_duplicates": None,
                    "drop_source_columns": False,
                    "event_source": None,
                    "layer": "SILVER",
                    "pipeline_name": "pl-stock-prices",
                    "table_source": {
                        "drops": None,
                        "filter": "created_at = '2023-09-01T00:00:00Z'",
                        "broadcast": False,
                        "read_as_stream": True,
                        "renames": None,
                        "selects": None,
                        "spark_chain": None,
                        "watermark": None,
                        "catalog_name": "dev",
                        "cdc": None,
                        "fmt": "DELTA",
                        "from_pipeline": True,
                        "name": "slv_stock_prices",
                        "path": None,
                        "schema_name": "markets",
                    },
                    "template": "SILVER",
                    "spark_chain": {
                        "nodes": [
                            {
                                "allow_missing_column_args": False,
                                "column": None,
                                "spark_func_args": [],
                                "spark_func_kwargs": {
                                    "other": {
                                        "value": {
                                            "drops": None,
                                            "filter": None,
                                            "broadcast": False,
                                            "read_as_stream": True,
                                            "renames": {"symbol2": "symbol"},
                                            "selects": None,
                                            "spark_chain": None,
                                            "watermark": None,
                                            "catalog_name": "dev",
                                            "cdc": None,
                                            "fmt": "DELTA",
                                            "from_pipeline": True,
                                            "name": "slv_stockmeta",
                                            "path": None,
                                            "schema_name": "markets",
                                        }
                                    },
                                    "on": {"value": ["symbol"]},
                                },
                                "spark_func_name": "smart_join",
                                "sql_expression": None,
                            },
                            {
                                "allow_missing_column_args": False,
                                "column": {
                                    "name": "symbol3",
                                    "type": "string",
                                    "unit": None,
                                },
                                "spark_func_args": [],
                                "spark_func_kwargs": {},
                                "spark_func_name": None,
                                "sql_expression": "symbol",
                            },
                            {
                                "allow_missing_column_args": False,
                                "column": None,
                                "spark_func_args": [{"value": "symbol"}],
                                "spark_func_kwargs": {},
                                "spark_func_name": "drop",
                                "sql_expression": None,
                            },
                            {
                                "allow_missing_column_args": False,
                                "column": None,
                                "spark_func_args": [],
                                "spark_func_kwargs": {
                                    "other": {
                                        "value": {
                                            "drops": None,
                                            "filter": None,
                                            "broadcast": False,
                                            "read_as_stream": True,
                                            "renames": None,
                                            "selects": None,
                                            "spark_chain": None,
                                            "watermark": None,
                                            "catalog_name": "dev",
                                            "cdc": None,
                                            "fmt": "DELTA",
                                            "from_pipeline": True,
                                            "name": "slv_stock_names",
                                            "path": None,
                                            "schema_name": "markets",
                                        }
                                    },
                                    "on": {"value": ["symbol3"]},
                                },
                                "spark_func_name": "smart_join",
                                "sql_expression": None,
                            },
                        ]
                    },
                },
                "catalog_name": "dev1",
                "columns": [],
                "comment": None,
                "data": None,
                "data_source_format": "DELTA",
                "expectations": [],
                "grants": None,
                "name": "slv_join_stock_prices",
                "primary_key": None,
                "schema_name": "markets1",
                "table_type": "MANAGED",
                "timestamp_key": None,
                "view_definition": None,
                "warehouse_id": "08b717ce051a0261",
            },
        ],
        "target": "markets1",
        "udfs": [
            {
                "module_name": "stock_functions",
                "function_name": "high",
                "module_path": None,
            }
        ],
    }


def test_pipeline_pulumi():
    assert pl.resource_name == "pl-stock-prices"
    assert pl.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
    }
    print(pl.pulumi_properties)
    assert pl.pulumi_properties == {
        "catalog": "dev1",
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "name": "pl-stock-prices",
        "notifications": [],
        "target": "markets1",
    }

    # Resources
    assert len(pl.core_resources) == 3
    r = pl.core_resources[-1]
    r.options.aliases = ["my-file"]
    assert pl.core_resources[-1].options.aliases == ["my-file"]


if __name__ == "__main__":
    test_pipeline()
    test_pipeline_pulumi()
