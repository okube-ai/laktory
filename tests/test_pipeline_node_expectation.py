import os
import shutil
import pytest
import uuid
from pathlib import Path
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
testdir_path = Path(__file__).parent


def get_node():

    return models.PipelineNode(
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
    )


def get_source(node_path):
    source_path = str(node_path / "brz_stock_prices")
    # w = Window.orderBy("data.created_at", "data.symbol")
    w = Window.orderBy("data.symbol", "data.created_at")
    source = df_brz.withColumn("index", F.row_number().over(w) - 1)
    return source, source_path


def test_warn():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="WARN",
        )
    ]
    node.execute()
    o = node.output_df.toPandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 8
    assert len(o) == 80


def test_drop():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="DROP",
        )
    ]
    node.execute()
    o = node.output_df.toPandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 8
    assert len(o) == 72


def test_quarantine():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="QUARANTINE",
        )
    ]
    node.execute()
    o = node.output_df.toPandas()
    q = node.quarantine_df.toPandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 8
    assert len(o) == 72
    assert len(q) == 8
    assert o["close"].max() < 330
    assert q["close"].min() >= 330


def test_fail():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="FAIL",
        )
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 8


def test_aggregate():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="rows count",
            expr="count(*) > 100",
            action="FAIL",
            type="AGGREGATE",
        )
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count is None


def test_multi():

    node = get_node()
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="QUARANTINE",
        ),
        models.DataQualityExpectation(
            name="not amazon",
            expr="symbol != 'AMZN'",
            action="DROP",
        ),
    ]
    node.execute()
    o = node.output_df.toPandas()
    q = node.quarantine_df.toPandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 80
    assert node.checks[0].fails_count == 8
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 80
    assert node.checks[1].fails_count == 20
    assert len(o) == 52
    assert len(q) == 8
    assert o["close"].max() < 330
    assert q["close"].min() >= 330


def test_streaming_multi():

    node_path = (
        testdir_path / "tmp" / "test_pipeline_node_expectations" / str(uuid.uuid4())
    )
    checkpoint_path = node_path / "_checkpoint_expectations"

    # Cleanup
    if os.path.exists(str(node_path)):
        shutil.rmtree(str(node_path))

    # Create Stream Source
    source, source_path = get_source(node_path)

    # Insert a single row
    source.filter("index=0").write.format("delta").mode("OVERWRITE").save(source_path)

    # Get Node
    node = get_node()
    node.source = models.FileDataSource(
        path=source_path,
        format="DELTA",
        as_stream=True,
    )
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="close < 300",
            action="WARN",
        ),
        models.DataQualityExpectation(
            name="max price",
            expr="close < 330",
            action="QUARANTINE",
        ),
        models.DataQualityExpectation(
            name="not amazon",
            expr="symbol != 'AMZN'",
            action="DROP",
        ),
    ]
    node.expectations_checkpoint_location = checkpoint_path

    # Execute
    node.execute(spark=spark)

    # Test
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
    source.filter("index>0 AND index<40").write.format("delta").mode("append").save(
        source_path
    )
    node.execute(spark=spark)
    print(node.checks)
    assert node.checks[0].status == "PASS"
    assert node.checks[0].rows_count == 39
    assert node.checks[0].fails_count == 0
    assert node.checks[1].status == "PASS"
    assert node.checks[1].rows_count == 39
    assert node.checks[1].fails_count == 0
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 39
    assert node.checks[2].fails_count == 20

    source.filter("index>=40").write.format("delta").mode("append").save(source_path)
    node.execute(spark=spark)
    print(node.checks)
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 40
    assert node.checks[0].fails_count == 20
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 40
    assert node.checks[1].fails_count == 8
    assert node.checks[2].status == "PASS"
    assert node.checks[2].rows_count == 40
    assert node.checks[2].fails_count == 0

    # Raise Exception
    source.filter("index>=40").write.format("delta").mode("append").save(source_path)
    node.expectations[0].action = "FAIL"
    # node.execute(spark=spark)
    # # TODO: Find how to capture exception
    # # with pytest.raises(Exception):
    # #     node.execute(spark=spark)


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

    # with pytest.warns(UserWarning):
    #     node = models.PipelineNode(
    #         name="slv_stock_prices",
    #         source={
    #             "path": "some_path",
    #             "format": "DELTA",
    #             "as_stream": True,
    #         },
    #         expectations=[
    #             {
    #                 "name": "max price pass",
    #                 "expr": "F.('close') < 300",
    #             },
    #         ],
    #     )


if __name__ == "__main__":
    test_warn()
    test_drop()
    test_quarantine()
    test_fail()
    test_aggregate()
    test_multi()
    test_streaming_multi()
    test_expectations_invalid()
