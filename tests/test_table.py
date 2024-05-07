import pytest
from pydantic import ValidationError

from laktory.models import Table
from laktory._testing.stockprices import table_slv
from laktory._testing.stockprices import table_slv_join
from laktory._testing.stockprices import spark


def test_model():
    print(table_slv.model_dump())
    data = table_slv.model_dump()
    assert data == {
        "builder": {
            "add_laktory_columns": True,
            "as_dlt_view": False,
            "drop_duplicates": None,
            "drop_source_columns": True,
            "event_source": None,
            "layer": "SILVER",
            "pipeline_name": None,
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
        "data": [
            ["2023-11-01T00:00:00Z", "AAPL", 1, 2],
            ["2023-11-01T01:00:00Z", "AAPL", 3, 4],
            ["2023-11-01T00:00:00Z", "GOOGL", 3, 4],
            ["2023-11-01T01:00:00Z", "GOOGL", 5, 6],
        ],
        "data_source_format": "DELTA",
        "expectations": [
            {"name": "positive_price", "expression": "open > 0", "action": "FAIL"},
            {
                "name": "recent_price",
                "expression": "created_at > '2023-01-01'",
                "action": "DROP",
            },
        ],
        "grants": None,
        "name": "slv_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "table_type": "MANAGED",
        "timestamp_key": None,
        "view_definition": None,
        "warehouse_id": "08b717ce051a0261",
    }

    assert not table_slv.is_from_cdc

    assert table_slv.warning_expectations == {}
    assert table_slv.drop_expectations == {"recent_price": "created_at > '2023-01-01'"}
    assert table_slv.fail_expectations == {"positive_price": "open > 0"}

    # Invalid layer
    with pytest.raises(ValidationError):
        Table(name="googl", layer="ROUGE")


def test_data():
    data = table_slv.to_df().to_dict(orient="records")
    print(data)
    assert data == [
        {"created_at": "2023-11-01T00:00:00Z", "symbol": "AAPL", "open": 1, "close": 2},
        {"created_at": "2023-11-01T01:00:00Z", "symbol": "AAPL", "open": 3, "close": 4},
        {
            "created_at": "2023-11-01T00:00:00Z",
            "symbol": "GOOGL",
            "open": 3,
            "close": 4,
        },
        {
            "created_at": "2023-11-01T01:00:00Z",
            "symbol": "GOOGL",
            "open": 5,
            "close": 6,
        },
    ]


def test_table_builder():
    # Test model
    data = table_slv_join.model_dump()
    print(data)
    assert data == {
        "builder": {
            "add_laktory_columns": True,
            "as_dlt_view": False,
            "drop_duplicates": None,
            "drop_source_columns": False,
            "event_source": None,
            "layer": "SILVER",
            "pipeline_name": None,
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
                        "column": {"name": "symbol3", "type": "string", "unit": None},
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
        "catalog_name": "dev",
        "columns": [],
        "comment": None,
        "data": None,
        "data_source_format": "DELTA",
        "expectations": [],
        "grants": None,
        "name": "slv_join_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "table_type": "MANAGED",
        "timestamp_key": None,
        "view_definition": None,
        "warehouse_id": "08b717ce051a0261",
    }

    # Test process
    df = table_slv_join.builder.read_source(spark)
    df = table_slv_join.builder.process(df, spark=spark)
    df = df.sort("symbol3")
    pdf = df.toPandas()

    # Test data
    assert df.columns == [
        "created_at",
        "open",
        "close",
        "currency",
        "first_traded",
        "name",
        "symbol3",
        "_silver_at",
    ]
    assert df.count() == 4
    assert pdf["symbol3"].tolist() == ["AAPL", "AMZN", "GOOGL", "MSFT"]
    assert pdf["name"].tolist() == ["Apple", "Amazon", "Google", None]


if __name__ == "__main__":
    test_model()
    test_data()
    test_table_builder()
