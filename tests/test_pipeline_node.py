import os
import shutil

import pytest
from pydantic_core._pydantic_core import ValidationError
from pyspark.sql import functions as F
from pyspark.sql import Window

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths
from laktory._testing import df_brz
from laktory.exceptions import DataQualityCheckFailedError
from laktory.exceptions import DataQualityExpectationsNotSupported

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


def test_expectations():

    # Test Warn / Drop
    node = models.PipelineNode(
        name="slv_stock_prices",
        source={
            "table_name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        drop_source_columns=True,
        transformer={
            "nodes": [
                {
                    "with_columns": [
                        {
                            "name": "symbol",
                            "expr": "data.symbol",
                        },
                        {
                            "name": "close",
                            "expr": "data.close",
                            "type": "double",
                        },
                    ]
                },
            ],
        },
        expectations=[
            {
                "name": "max price pass",
                "expr": "close < 300",
                "action": "WARN",
            },
            {
                "name": "max price drop",
                "expr": "close < 325",
                "action": "DROP",
            },
            {
                "name": "max price drop",
                "expr": "close < 330",
                "action": "QUARANTINE",
            },
        ],
    )
    node.execute()
    o = node.output_df.toPandas()
    q = node.quarantine_df.toPandas()

    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 20
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 80
    assert node.checks[1].fails_count == 12
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 80
    assert node.checks[2].fails_count == 8
    assert len(o) == 68
    assert len(q) == 8
    assert o["close"].max() < 325
    assert q["close"].min() >= 330

    # Test Fail
    node.expectations = [
        models.DataQualityExpectation(
            name="not Apple",
            expr="symbol != 'AAPL'",
            action="FAIL",
        ),
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"

    # Test Aggregate
    node.expectations = [
        models.DataQualityExpectation(
            name="rows count",
            expr="count(*) > 100",
            type="AGGREGATE",
            action="FAIL",
        ),
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"


def test_expectations_streaming():

    w = Window.orderBy("data.symbol", "data.created_at")
    _df_brz = df_brz.withColumn("index", F.row_number().over(w) - 1)

    # Create Stream Source
    source_path = os.path.join(paths.tmp, "streamsource/")
    checkpoint_path = os.path.join(paths.tmp, "streamsource/checkpoint")
    if os.path.exists(source_path):
        shutil.rmtree(source_path)
    _df_brz.filter("index=0").write.format("delta").mode("OVERWRITE").save(source_path)

    # Test Warn / Drop
    node = models.PipelineNode(
        name="slv_stock_prices",
        source={
            "path": source_path,
            "format": "DELTA",
            "as_stream": True,
        },
        drop_source_columns=True,
        transformer={
            "nodes": [
                {
                    "with_columns": [
                        {
                            "name": "symbol",
                            "expr": "data.symbol",
                        },
                        {
                            "name": "close",
                            "expr": "data.close",
                            "type": "double",
                        },
                    ]
                },
            ],
        },
        expectations=[
            {
                "name": "max price pass",
                "expr": "close < 300",
                "action": "WARN",
            },
            {
                "name": "max price drop",
                "expr": "close < 325",
                "action": "DROP",
            },
            {
                "name": "max price drop",
                "expr": "close < 330",
                "action": "QUARANTINE",
            },
        ],
        expectations_checkpoint_location=checkpoint_path,
    )
    node.execute(spark=spark)
    assert node.checks[0].status == "PASS"
    assert node.checks[0].rows_count == 1
    assert node.checks[0].fails_count == 0
    assert node.checks[1].status == "PASS"
    assert node.checks[1].rows_count == 1
    assert node.checks[1].fails_count == 0
    assert node.checks[2].status == "PASS"
    assert node.checks[2].rows_count == 1
    assert node.checks[2].fails_count == 0

    # Update source
    _df_brz.filter("index>0 AND index<40").write.format("delta").mode("append").save(
        source_path
    )
    node.execute(spark=spark)
    assert node.checks[0].status == "PASS"
    assert node.checks[0].rows_count == 39
    assert node.checks[0].fails_count == 0
    assert node.checks[1].status == "PASS"
    assert node.checks[1].rows_count == 39
    assert node.checks[1].fails_count == 0
    assert node.checks[2].status == "PASS"
    assert node.checks[2].rows_count == 39
    assert node.checks[2].fails_count == 0

    _df_brz.filter("index>=40").write.format("delta").mode("append").save(source_path)
    node.execute(spark=spark)
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 40
    assert node.checks[0].fails_count == 20
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 40
    assert node.checks[1].fails_count == 12
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 40
    assert node.checks[2].fails_count == 8

    # Change action
    node.expectations[2].action = "FAIL"
    node.execute(spark=spark)
    _df_brz.filter("index>=40").write.format("delta").mode("append").save(source_path)
    # TODO: Find how to capture exception
    # with pytest.raises(Exception):
    #     node.execute(spark=spark)


def test_expectations_invalid():

    with pytest.raises(DataQualityExpectationsNotSupported):
        node = models.PipelineNode(
            name="slv_stock_prices",
            source={
                "path": "some_path",
                "format": "DELTA",
                "as_stream": True,
            },
            expectations=[
                {
                    "name": "max price pass",
                    "expr": "close < 300",
                    "action": "WARN",
                },
                {
                    "name": "max price drop",
                    "expr": "count(*) > 20",
                    "type": "AGGREGATE",
                },
            ],
            expectations_checkpoint_location="some_path",
        )

    with pytest.raises(DataQualityExpectationsNotSupported):
        node = models.PipelineNode(
            name="slv_stock_prices",
            source={
                "path": "some_path",
                "format": "DELTA",
                "as_stream": True,
            },
            expectations=[
                {
                    "name": "max price pass",
                    "expr": "close < 300",
                    "action": "WARN",
                    "tolerance": {"abs": 20},
                },
            ],
            expectations_checkpoint_location="some_path",
        )

    with pytest.warns(UserWarning):
        node = models.PipelineNode(
            name="slv_stock_prices",
            source={
                "path": "some_path",
                "format": "DELTA",
                "as_stream": True,
            },
            expectations=[
                {
                    "name": "max price pass",
                    "expr": "F.('close') < 300",

                },
            ],
        )


if __name__ == "__main__":
    test_execute()
    test_bronze()
    test_silver()
    test_cdc()
    test_expectations()
    test_expectations_streaming()
    test_expectations_invalid()
