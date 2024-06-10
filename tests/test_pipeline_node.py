import os
import shutil

from pyspark.sql import functions as F

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths

paths = Paths(__file__)

# Data
df_brz = spark.read.parquet(os.path.join(paths.data, "./brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(paths.data, "./slv_stock_prices"))


def test_execute():

    sink_path = os.path.join(paths.tmp, "pl_node_sink")

    node = models.PipelineNode(
        name="slv_stock_prices",
        source={
            "table_name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        transformer={
            "spark": True,
            "nodes": [
                {
                    "with_column": {
                        "name": "created_at",
                        "type": "timestamp",
                        "sql_expr": "data.created_at",
                    },
                },
                {
                    "with_column": {"name": "symbol", "sql_expr": "data.symbol"},
                },
                {
                    "with_column": {
                        "name": "close",
                        "type": "double",
                        "sql_expr": "data.close",
                    },
                },
                {
                    "func_name": "drop",
                    "func_args": ["data", "producer", "name", "description"],
                },
            ],
        },
        sink={
            "path": sink_path,
            "format": "PARQUET",
            "mode": "OVERWRITE",
        },
    )
    df0 = node.execute()
    df1 = spark.read.format("PARQUET").load(sink_path)

    assert df1.columns == df0.columns
    assert df1.columns == ["created_at", "symbol", "close"]
    assert df1.count() == df_brz.count()

    # Cleanup
    shutil.rmtree(sink_path)


def test_bronze():
    node = models.PipelineNode(
        layer="BRONZE",
        name="slv_stock_prices",
        source={
            "table_name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        transformer={
            "nodes": [
                {
                    "with_column": {"name": "symbol", "sql_expr": "data.symbol"},
                },
            ]
        },
    )

    # Read and process
    df = node.execute(spark)

    # Test
    assert not node.drop_source_columns
    assert not node.drop_duplicates
    assert node.layer == "BRONZE"
    assert node.add_layer_columns
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

    node = models.PipelineNode(
        layer="SILVER",
        source={
            "table_name": "slv_stock_prices",
            "mock_df": df,
        },
        transformer={
            "nodes": [
                {
                    "with_column": {
                        "name": "symbol",
                        "type": "string",
                        "sql_expr": "data.symbol",
                    },
                },
            ]
        },
        drop_duplicates=True,
    )

    # Read and process
    df = node.execute(spark)

    # Test
    assert node.drop_source_columns
    assert node.drop_duplicates
    assert node.layer == "SILVER"
    assert node.add_layer_columns
    assert df.columns == ["_bronze_at", "symbol", "_silver_at"]
    assert df.count() == 80


def test_cdc():
    node = models.PipelineNode(
        source={
            "table_name": "brz_users_type1",
            "cdc": {
                "primary_keys": ["userId"],
                "sequence_by": "sequenceNum",
                "apply_as_deletes": "operation = 'DELETE'",
                "scd_type": 1,
                "except_columns": ["operation", "sequenceNum"],
            },
        },
    )

    # TODO: Test CDC transformations when ready
    print(node)


#
#     assert table.builder.apply_changes_kwargs == {
#         "apply_as_deletes": "operation = 'DELETE'",
#         "apply_as_truncates": None,
#         "column_list": [],
#         "except_column_list": ["operation", "sequenceNum"],
#         "ignore_null_updates": None,
#         "keys": ["userId"],
#         "sequence_by": "sequenceNum",
#         "source": "brz_users_cdc",
#         "stored_as_scd_type": 1,
#         "target": "brz_users_type1",
#         "track_history_column_list": None,
#         "track_history_except_column_list": None,
#     }
#     assert table.builder.is_from_cdc
#
#     # TODO: Run test with demo data
#     # from pyspark.sql import SparkSession
#
#     # spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
#     #
#     # df_cdc = spark.createDataFrame(pd.DataFrame({
#     #     "userId": [124, 123, 125, 126, 123, 125, 125, 123],
#     #     "name": ["Raul", "Isabel", "Mercedes", "Lily", None, "Mercedes", "Mercedes", "Isabel"],
#     #     "city": ["Oaxaca", "Monterrey", "Tijuana", "Cancun", None, "Guadalajara", "Mexicali", "Chihuahua"],
#     #     "operation": ["INSERT", "INSERT", "INSERT", "INSERT", "DELETE", "UPDATE", "UPDATE", "UPDATE"],
#     #     "sequenceNum": [1, 1, 2, 2, 6, 6, 5, 5],
#     # }))
#     # df_cdc.show()


if __name__ == "__main__":
    test_execute()
    test_bronze()
    test_silver()
    test_cdc()
