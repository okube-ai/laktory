import os
import pandas as pd
from pyspark.sql import SparkSession

from laktory.models import Table

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# DataFrames                                                                  #
# --------------------------------------------------------------------------- #

data_dirpath = os.path.join(os.path.dirname(__file__), "../../tests/data/")
df_brz = spark.read.parquet(os.path.join(data_dirpath, "brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(data_dirpath, "slv_stock_prices"))
df_meta = spark.read.parquet(os.path.join(data_dirpath, "slv_stock_meta"))
df_name = spark.createDataFrame(
    pd.DataFrame(
        {
            "symbol3": ["AAPL", "GOOGL", "AMZN"],
            "name": ["Apple", "Google", "Amazon"],
        }
    )
)

# --------------------------------------------------------------------------- #
# Tables                                                                      #
# --------------------------------------------------------------------------- #

table_slv = Table(
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

table_slv_join = Table(
    name="slv_join_stock_prices",
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
