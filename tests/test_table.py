from pydantic import ValidationError
from pyspark.sql import types as T
import pandas as pd
import pytest

from laktory.models import Table
from laktory._testing import table_brz
from laktory._testing import table_slv
from laktory._testing import EventsManager

manager = EventsManager()
manager.build_events()


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
        "event_source": None,
        "grants": None,
        "name": "slv_stock_prices",
        "pipeline_name": None,
        "primary_key": None,
        "schema_name": "markets",
        "table_source": {
            "read_as_stream": True,
            "catalog_name": "dev",
            "cdc": None,
            "from_pipeline": True,
            "name": "brz_stock_prices",
            "schema_name": "markets",
            "watermark": None,
            "where": None,
        },
        "table_join_source": None,
        "timestamp_key": None,
        "zone": "SILVER",
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
    df1 = table_brz.process_bronze(df0)
    assert "_bronze_at" in df1.columns


def test_silver():
    df0 = manager.to_spark_df()
    df0.show()
    df1 = table_brz.process_bronze(df0)
    df1.show()
    df2 = table_slv.process_silver(df1)
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


def test_silver_star():
    df0 = manager.to_spark_df()
    df0.show()
    df1 = table_brz.process_bronze(df0)
    df1.show()
    df2 = table_slv.process_silver(df1)
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


def test_cdc():
    table = Table(
        name="brz_users_type1",
        table_source={
            "name": "brz_users_cdc",
            "cdc": {
                "primary_keys": ["userId"],
                "sequence_by": "sequenceNum",
                "apply_as_deletes": "operation = 'DELETE'",
                "scd_type": 1,
                "except_columns": ["operation", "sequenceNum"],
            },
        },
    )

    assert table.apply_changes_kwargs == {
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
    assert table.is_from_cdc

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
    # test_silver_star()
    test_cdc()
