import os
import pytest
import pandas as pd
from pydantic import ValidationError
from pyspark.sql import types as T
from pyspark.sql import SparkSession

from laktory.models import Table

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

dirpath = os.path.dirname(__file__)
df_brz = spark.read.parquet(os.path.join(dirpath, "./data/brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(dirpath, "./data/slv_stock_prices"))
df_meta = spark.read.parquet(os.path.join(dirpath, "./data/slv_stock_meta"))

table_slv = Table(
    name="slv_stock_prices",
    columns=[
        {
            "name": "created_at",
            "type": "timestamp",
            # "spark_func_name": "coalesce",
            # "spark_func_args": ["data._created_at"],
        },
        {
            "name": "symbol",
            "type": "string",
            # "spark_func_name": "coalesce",
            # "spark_func_args": ["data.symbol"],
        },
        {
            "name": "open",
            "type": "double",
            # "spark_func_name": "coalesce",
            # "spark_func_args": ["data.open"],
        },
        {
            "name": "close",
            "type": "double",
            # "sql_expression": "data.open"
        },
    ],
    data=[
        ["2023-11-01T00:00:00Z", "AAPL", 1, 2],
        ["2023-11-01T01:00:00Z", "AAPL", 3, 4],
        ["2023-11-01T00:00:00Z", "GOOGL", 3, 4],
        ["2023-11-01T01:00:00Z", "GOOGL", 5, 6],
    ],
    catalog_name="dev",
    schema_name="markets",
    builder={
        "table_source": {
            "name": "brz_stock_prices",
        },
        "layer": "SILVER",
    },
    expectations=[
        {"name": "positive_price", "expression": "open > 0", "action": "FAIL"},
        {
            "name": "recent_price",
            "expression": "created_at > '2023-01-01'",
            "action": "DROP",
        },
    ],
)


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
                "read_as_stream": True,
                "renames": None,
                "selects": None,
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
    df_name = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol3": ["AAPL", "GOOGL", "AMZN"],
                "name": ["Apple", "Google", "Amazon"],
            }
        )
    )

    table = Table(
        name="slv_star_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        builder={
            "layer": "SILVER",
            "table_source": {
                "mock_df": df_slv,
                "name": "slv_stock_prices",
                "filter": "created_at = '2023-09-01T00:00:00Z'",
            },
            "spark_chain": {
                "nodes": [
                    {
                        "spark_func_name": "laktory_join",
                        "spark_func_kwargs": {
                            "other": {
                                "name": "slv_stockmeta",
                                "mock_df": df_meta,
                                "renames": {"symbol2": "symbol"},
                            },
                            "on": ["symbol"],
                        },
                    },
                    {"name": "symbol3", "sql_expression": "symbol"},
                    {
                        "spark_func_name": "drop",
                        "spark_func_args": ["symbol"],
                    },
                    {
                        "spark_func_name": "laktory_join",
                        "spark_func_kwargs": {
                            "other": {
                                "name": "slv_stock_names",
                                "mock_df": df_name,
                            },
                            "on": ["symbol3"],
                        },
                    },
                ]
            },
            "drop_source_columns": False,
        },
    )

    # Test model
    data = table.model_dump()
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
                "read_as_stream": True,
                "renames": None,
                "selects": None,
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
                        "spark_func_args": [],
                        "spark_func_kwargs": {
                            "other": {
                                "value": {
                                    "drops": None,
                                    "filter": None,
                                    "read_as_stream": True,
                                    "renames": {"symbol2": "symbol"},
                                    "selects": None,
                                    "watermark": None,
                                    "catalog_name": None,
                                    "cdc": None,
                                    "fmt": "DELTA",
                                    "from_pipeline": True,
                                    "name": "slv_stockmeta",
                                    "path": None,
                                    "schema_name": None,
                                }
                            },
                            "on": {"value": ["symbol"]},
                        },
                        "spark_func_name": "laktory_join",
                    },
                    {
                        "allow_missing_column_args": False,
                        "name": "symbol3",
                        "spark_func_args": [],
                        "spark_func_kwargs": {},
                        "spark_func_name": None,
                        "sql_expression": "symbol",
                        "type": "string",
                        "unit": None,
                    },
                    {
                        "allow_missing_column_args": False,
                        "spark_func_args": [{"value": "symbol"}],
                        "spark_func_kwargs": {},
                        "spark_func_name": "drop",
                    },
                    {
                        "allow_missing_column_args": False,
                        "spark_func_args": [],
                        "spark_func_kwargs": {
                            "other": {
                                "value": {
                                    "drops": None,
                                    "filter": None,
                                    "read_as_stream": True,
                                    "renames": None,
                                    "selects": None,
                                    "watermark": None,
                                    "catalog_name": None,
                                    "cdc": None,
                                    "fmt": "DELTA",
                                    "from_pipeline": True,
                                    "name": "slv_stock_names",
                                    "path": None,
                                    "schema_name": None,
                                }
                            },
                            "on": {"value": ["symbol3"]},
                        },
                        "spark_func_name": "laktory_join",
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
        "name": "slv_star_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "table_type": "MANAGED",
        "timestamp_key": None,
        "view_definition": None,
        "warehouse_id": None,
    }

    # Test process
    df = table.builder.read_source(spark)
    df = table.builder.process(df, spark=spark)
    df = df.sort("symbol3")
    pdf = df.toPandas()

    # Test data
    assert df.columns == ['created_at', 'open', 'close', 'currency', 'first_traded', 'name', 'symbol3', '_silver_at']
    assert df.count() == 4
    assert pdf["symbol3"].tolist() == ["AAPL", "AMZN", "GOOGL", "MSFT"]
    assert pdf["name"].tolist() == ["Apple", "Amazon", "Google", None]


if __name__ == "__main__":
    test_model()
    test_data()
    test_table_builder()
