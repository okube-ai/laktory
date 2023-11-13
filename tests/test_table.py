from pydantic import ValidationError
from pyspark.sql import types as T
from pyspark.sql import SparkSession
import pandas as pd
from pandas import Timestamp
import pytest

from laktory._testing import EventsManager
from laktory._testing import df_meta
from laktory._testing import table_brz
from laktory._testing import table_slv
from laktory._testing import table_slv_star
from laktory.models import Table
from laktory.models import TableJoin

manager = EventsManager()
manager.build_events()

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()


def test_model():
    print(table_slv.model_dump())
    assert table_slv.model_dump() == {
        "catalog_name": "dev",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "created_at",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "_created_at", "to_column": True, "to_lit": False},
                    {"value": "data._created_at", "to_column": True, "to_lit": False},
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "timestamp",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol",
                "pii": None,
                "schema_name": "markets",
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
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "schema_name": "markets",
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
                "catalog_name": "dev",
                "comment": None,
                "name": "close",
                "pii": None,
                "schema_name": "markets",
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
        "grants": None,
        "name": "slv_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "timestamp_key": None,
        "builder": {
            "drop_source_columns": True,
            "drop_duplicates": None,
            "event_source": None,
            "joins": [],
            "pipeline_name": None,
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
            "zone": "SILVER",
        },
    }

    assert not table_slv.is_from_cdc

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")


def test_data():
    data = table_slv.df.to_dict(orient="records")
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


def test_bronze():
    df0 = manager.to_spark_df()
    df1 = table_brz.builder.process(df0)
    assert "_bronze_at" in df1.columns


def test_silver():
    df0 = manager.to_spark_df()
    df0.show()
    df1 = table_brz.builder.process(df0)
    df1.show()
    df2 = table_slv.builder.process(df1)
    df2.show()
    assert df2.schema == T.StructType(
        [
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("open", T.DoubleType(), True),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("_bronze_at", T.TimestampType(), False),
            T.StructField("_silver_at", T.TimestampType(), False),
        ]
    )
    s = df2.toPandas().iloc[0]
    print(s)
    assert s["created_at"] == pd.Timestamp("2023-09-01 00:00:00")
    assert s["symbol"] == "AAPL"
    assert s["open"] == 189.49000549316406
    assert s["close"] == 189.49000549316406


def test_table_join():
    join = TableJoin(
        left={
            "name": "slv_stock_prices",
            "filter": "created_at = '2023-11-01T00:00:00Z'",
        },
        other={
            "name": "slv_stock_metadata",
            "selects": {
                "symbol2": "symbol",
                "currency": "currency",
                "first_traded": "first_traded",
            },
        },
        on=["symbol"],
    )
    join.left._df = table_slv.to_df(spark)
    join.other._df = df_meta

    df = join.run(spark)
    data = df.toPandas().to_dict(orient="records")
    print(data)
    assert data == [
        {
            "created_at": "2023-11-01T00:00:00Z",
            "symbol": "AAPL",
            "open": 1,
            "close": 2,
            "currency": "USD",
            "first_traded": "1980-12-12T14:30:00.000Z",
        },
        {
            "created_at": "2023-11-01T00:00:00Z",
            "symbol": "GOOGL",
            "open": 3,
            "close": 4,
            "currency": "USD",
            "first_traded": "2004-08-19T13:30:00.00Z",
        },
    ]


def test_silver_star():
    print(table_slv_star.model_dump())
    assert table_slv_star.model_dump() == {
        "catalog_name": "dev",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol3",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "symbol", "to_column": True, "to_lit": False}
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_star_stock_prices",
                "type": "string",
                "unit": None,
            }
        ],
        "comment": None,
        "data": None,
        "grants": None,
        "name": "slv_star_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "timestamp_key": None,
        "builder": {
            "drop_source_columns": False,
            "drop_duplicates": None,
            "event_source": None,
            "joins": [
                {
                    "left": None,
                    "other": {
                        "read_as_stream": True,
                        "catalog_name": "dev",
                        "cdc": None,
                        "selects": {
                            "symbol2": "symbol",
                            "currency": "currency",
                            "first_traded": "last_traded",
                        },
                        "filter": None,
                        "from_pipeline": True,
                        "name": "slv_stock_metadata",
                        "schema_name": "markets",
                        "watermark": None,
                    },
                    "on": ["symbol"],
                    "how": "left",
                    "time_constraint_interval_lower": "60 seconds",
                    "time_constraint_interval_upper": None,
                },
                {
                    "left": None,
                    "other": {
                        "read_as_stream": True,
                        "catalog_name": "dev",
                        "cdc": None,
                        "selects": ["symbol3", "name"],
                        "filter": None,
                        "from_pipeline": True,
                        "name": "slv_stock_names",
                        "schema_name": "markets",
                        "watermark": None,
                    },
                    "on": ["symbol3"],
                    "how": "left",
                    "time_constraint_interval_lower": "60 seconds",
                    "time_constraint_interval_upper": None,
                },
            ],
            "pipeline_name": None,
            "table_source": {
                "read_as_stream": True,
                "catalog_name": "dev",
                "cdc": None,
                "selects": None,
                "filter": "created_at = '2023-11-01T00:00:00Z'",
                "from_pipeline": True,
                "name": "slv_stock_prices",
                "schema_name": "markets",
                "watermark": None,
            },
            "zone": "SILVER_STAR",
        },
    }

    df = table_slv_star.builder.read_source(spark)
    df = table_slv_star.builder.process(df, spark=spark)
    assert "_silver_star_at" in df.columns
    data = df.toPandas().drop("_silver_star_at", axis=1).to_dict("records")
    print(data)
    assert data == [
        {
            "created_at": "2023-11-01T00:00:00Z",
            "symbol": "AAPL",
            "open": 1,
            "close": 2,
            "symbol3": "AAPL",
            "currency": "USD",
            "last_traded": "1980-12-12T14:30:00.000Z",
            "name": "Apple",
        },
        {
            "created_at": "2023-11-01T00:00:00Z",
            "symbol": "GOOGL",
            "open": 3,
            "close": 4,
            "symbol3": "GOOGL",
            "currency": "USD",
            "last_traded": "2004-08-19T13:30:00.00Z",
            "name": "Google",
        },
    ]


def test_cdc():
    table = Table(
        name="brz_users_type1",
        builder={
            "table_source": {
                "name": "brz_users_cdc",
                "cdc": {
                    "primary_keys": ["userId"],
                    "sequence_by": "sequenceNum",
                    "apply_as_deletes": "operation = 'DELETE'",
                    "scd_type": 1,
                    "except_columns": ["operation", "sequenceNum"],
                },
            },
        },
    )

    assert table.builder.apply_changes_kwargs == {
        "apply_as_deletes": "operation = 'DELETE'",
        "apply_as_truncates": None,
        "column_list": [],
        "except_column_list": ["operation", "sequenceNum"],
        "ignore_null_updates": None,
        "keys": ["userId"],
        "sequence_by": "sequenceNum",
        "source": "brz_users_cdc",
        "stored_as_scd_type": 1,
        "target": "brz_users_type1",
        "track_history_column_list": None,
        "track_history_except_column_list": None,
    }
    assert table.builder.is_from_cdc

    # TODO: Run test with demo data
    # from pyspark.sql import SparkSession

    # spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
    #
    # df_cdc = spark.createDataFrame(pd.DataFrame({
    #     "userId": [124, 123, 125, 126, 123, 125, 125, 123],
    #     "name": ["Raul", "Isabel", "Mercedes", "Lily", None, "Mercedes", "Mercedes", "Isabel"],
    #     "city": ["Oaxaca", "Monterrey", "Tijuana", "Cancun", None, "Guadalajara", "Mexicali", "Chihuahua"],
    #     "operation": ["INSERT", "INSERT", "INSERT", "INSERT", "DELETE", "UPDATE", "UPDATE", "UPDATE"],
    #     "sequenceNum": [1, 1, 2, 2, 6, 6, 5, 5],
    # }))
    # df_cdc.show()


if __name__ == "__main__":
    test_model()
    test_data()
    test_bronze()
    test_silver()
    test_table_join()
    test_silver_star()
    test_cdc()
