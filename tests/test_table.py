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
from laktory._testing import table_gld
from laktory.models import Table
from laktory.models import TableJoin
from laktory.models import TableAggregation

manager = EventsManager()
manager.build_events()

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()


def test_model():
    print(table_slv.model_dump())
    assert table_slv.model_dump() == {
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
            "pipeline_name": None,
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
        "catalog_name": "dev",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "created_at",
                "pii": None,
                "schema_name": "markets",
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
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol",
                "pii": None,
                "schema_name": "markets",
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
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "schema_name": "markets",
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

    df = join.execute(spark)
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


def test_table_agg():
    agg = TableAggregation(
        groupby_columns=["symbol"],
        agg_expressions=[
            {"name": "min_open", "spark_func_name": "min", "spark_func_args": ["open"]},
            {
                "name": "max_open",
                "sql_expression": "max(open)",
            },
        ],
    )
    df = table_slv.to_df(spark)

    df1 = agg.execute(df=df)
    df1.show()
    data = df1.toPandas().to_dict(orient="records")
    print(data)
    assert data == [
        {"symbol": "AAPL", "min_open": 1, "max_open": 3},
        {"symbol": "GOOGL", "min_open": 3, "max_open": 5},
    ]


def test_silver_star():
    print(table_slv_star.model_dump())
    assert table_slv_star.model_dump() == {
        "builder": {
            "aggregation": None,
            "drop_columns": [],
            "drop_duplicates": None,
            "drop_source_columns": False,
            "event_source": None,
            "filter": None,
            "joins": [
                {
                    "how": "left",
                    "left": None,
                    "on": ["symbol"],
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
                    "time_constraint_interval_lower": "60 seconds",
                    "time_constraint_interval_upper": None,
                },
                {
                    "how": "left",
                    "left": None,
                    "on": ["symbol3"],
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
                    "time_constraint_interval_lower": "60 seconds",
                    "time_constraint_interval_upper": None,
                },
            ],
            "joins_post_aggregation": [],
            "layer": "SILVER_STAR",
            "pipeline_name": None,
            "selects": None,
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
            "template": "SILVER_STAR",
            "window_filter": None,
        },
        "catalog_name": "dev",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol3",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {
                        "value": "symbol",
                        "is_column": True,
                        "to_lit": False,
                        "to_expr": True,
                    }
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
        "data_source_format": "DELTA",
        "expectations": [],
        "grants": None,
        "name": "slv_star_stock_prices",
        "primary_key": None,
        "schema_name": "markets",
        "table_type": "MANAGED",
        "timestamp_key": None,
        "view_definition": None,
        "warehouse_id": "08b717ce051a0261",
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


def test_gold():
    assert table_gld.builder.template == "GOLD1"
    df0 = manager.to_spark_df()
    df0.show()
    df1 = table_brz.builder.process(df0)
    df1.show()
    df2 = table_slv.builder.process(df1)
    df2.show()
    df3 = table_gld.builder.process(df2)
    df3.show()
    df3.printSchema()
    assert df3.schema == T.StructType(
        [
            T.StructField("min_open", T.DoubleType(), True),
            T.StructField("max_open", T.DoubleType(), True),
            T.StructField("min_close", T.DoubleType(), True),
            T.StructField("_gold_at", T.TimestampType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("name2", T.StringType(), True),
        ]
    )
    s = df3.toPandas().iloc[0]
    print(s)
    assert s["name2"] == "Apple"


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
    test_table_agg()
    test_silver_star()
    test_gold()
    test_cdc()
