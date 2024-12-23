import os
import shutil
import uuid
from pathlib import Path

from pyspark.sql import functions as F

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths
from laktory._testing import df_brz
from laktory._testing import df_slv

paths = Paths(__file__)


def test_execute():

    sink_path = os.path.join(paths.tmp, "pl_node_sink")

    node = models.PipelineNode(
        name="slv_stock_prices",
        source={
            "table_name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        transformer={
            "nodes": [
                {
                    "with_column": {
                        "name": "created_at",
                        "type": "timestamp",
                        "expr": "data.created_at",
                    },
                },
                {
                    "with_column": {"name": "symbol", "expr": "data.symbol"},
                },
                {
                    "with_column": {
                        "name": "close",
                        "type": "double",
                        "expr": "data.close",
                    },
                },
                {
                    "func_name": "drop",
                    "func_args": ["data", "producer", "name", "description"],
                },
            ],
        },
        sinks=[
            {
                "path": sink_path,
                "format": "PARQUET",
                "mode": "OVERWRITE",
            }
        ],
    )
    df0 = node.execute()
    df1 = spark.read.format("PARQUET").load(sink_path)

    assert df1.columns == df0.columns
    assert df1.columns == ["created_at", "symbol", "close"]
    assert df1.count() == df_brz.count()

    # Cleanup
    shutil.rmtree(sink_path)


def test_execute_view():

    # Create table
    table_path = Path(paths.tmp) / "hive" / f"slv_{str(uuid.uuid4())}"
    (
        df_slv.write.mode("OVERWRITE")
        .option("path", table_path)
        .saveAsTable("default.slv")
    )

    node1 = models.PipelineNode(
        name="slv_stock_prices",
        source={
            "schema_name": "default",
            "table_name": "slv",
        },
        transformer={
            "nodes": [
                {
                    "sql_expr": "SELECT * FROM {df} WHERE symbol = 'AAPL'",
                }
            ],
        },
        sinks=[
            {
                "schema_name": "default",
                "table_name": "slv_aapl",
                "table_type": "VIEW",
            }
        ],
    )
    node2 = node1.model_copy(deep=True)
    node2.transformer = None
    node2.sinks = [
        models.TableDataSink(
            schema_name="default",
            table_name="slv_amzn",
            view_definition="SELECT * FROM {df} WHERE symbol = 'AMZN'",
        ),
        models.TableDataSink(
            schema_name="default",
            table_name="slv_msft",
            view_definition="SELECT * FROM {df} WHERE symbol = 'MSFT'",
        ),
    ]
    node2.update_children()
    df1 = node1.execute(spark=spark).toPandas()
    node2.execute(spark=spark)
    df2 = node2.sinks[0].read(spark=spark).toPandas()
    df3 = spark.read.table("default.slv_msft").toPandas()

    # Test
    assert node1.is_view
    assert node2.is_view
    assert df1["symbol"].unique().tolist() == ["AAPL"]
    assert df2["symbol"].unique().tolist() == ["AMZN"]
    assert df3["symbol"].unique().tolist() == ["MSFT"]

    # Cleanup
    shutil.rmtree(table_path)


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
                    "with_column": {"name": "symbol", "expr": "data.symbol"},
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
                        "expr": "data.symbol",
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


if __name__ == "__main__":
    test_execute()
    test_execute_view()
    test_bronze()
    test_silver()
