import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from laktory import models

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dirpath = os.path.dirname(__file__)
df_brz = spark.read.parquet(os.path.join(dirpath, "./data/brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(dirpath, "./data/slv_stock_prices"))


def test_read_and_process():
    builder = models.TableBuilder(
        event_source={
            "name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        spark_chain={
            "nodes": [
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "sql_expression": "data._created_at",
                },
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
                {"name": "open", "type": "double", "sql_expression": "data.open"},
                {"name": "close", "type": "double", "sql_expression": "data.close"},
                {
                    "spark_func_name": "drop",
                    "spark_func_args": [
                        "description",
                        "data",
                        "producer",
                    ],
                },
            ]
        },
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert df.columns == ["name", "created_at", "symbol", "open", "close"]
    assert df.count() == 80


def test_bronze():
    builder = models.TableBuilder(
        layer="BRONZE",
        event_source={
            "name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        spark_chain={
            "nodes": [
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
            ]
        },
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert not builder.drop_source_columns
    assert not builder.drop_duplicates
    assert builder.layer == "BRONZE"
    assert builder.template == "BRONZE"
    assert builder.add_laktory_columns
    assert df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "symbol",
        "_bronze_at",
    ]
    assert df.count() == 80


def test_silver():
    df = df_brz.select(df_brz.columns)
    df = df.withColumn("_bronze_at", F.current_timestamp())
    df = df.union(df)

    builder = models.TableBuilder(
        layer="SILVER",
        table_source={
            "name": "slv_stock_prices",
            "mock_df": df,
        },
        spark_chain={
            "nodes": [
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
            ]
        },
        drop_duplicates=True,
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert builder.drop_source_columns
    assert builder.drop_duplicates
    assert builder.layer == "SILVER"
    assert builder.template == "SILVER"
    assert builder.add_laktory_columns
    assert df.columns == ["_bronze_at", "symbol", "_silver_at"]
    assert df.count() == 80


def test_cdc():
    table = models.Table(
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
    test_read_and_process()
    test_bronze()
    test_silver()
    test_cdc()
