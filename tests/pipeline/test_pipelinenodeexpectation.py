from pathlib import Path

import pytest

from laktory import models
from laktory._testing import Paths
from laktory._testing import get_df0
from laktory._testing import sparkf
from laktory.exceptions import DataQualityCheckFailedError

paths = Paths(__file__)
spark = sparkf.spark
testdir_path = Path(__file__).parent


@pytest.fixture
def node():
    node = models.PipelineNode(
        name="node0",
        transformer={
            "nodes": [
                {"name": "with_columns", "kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from df"},
            ]
        },
        expectations=[
            models.DataQualityExpectation(
                name="max price",
                expr="x1 < 3",
                action="WARN",
            )
        ],
    )
    return node


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_warn(backend, node):
    df0 = get_df0(backend)

    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="x1 < 3",
            action="WARN",
        )
    ]
    node.execute()
    o = node.output_df.collect().to_pandas()

    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert len(o) == 3


#
#
#
# def get_node():
#     return models.PipelineNode(
#         name="slv_stock_prices",
#         source={
#             "table_name": "brz_stock_prices",
#             "mock_df": dff.brz,
#         },
#         drop_source_columns=True,
#         transformer={
#             "nodes": [
#                 {
#                     "with_columns": [
#                         {
#                             "name": "symbol",
#                             "expr": "data.symbol",
#                         },
#                         {
#                             "name": "close",
#                             "expr": "data.close",
#                             "type": "double",
#                         },
#                     ]
#                 },
#             ],
#         },
#     )

#
# def get_source(node_path):
#     source_path = str(node_path / "brz_stock_prices")
#     # w = Window.orderBy("data.created_at", "data.symbol")
#     w = Window.orderBy("data.symbol", "data.created_at")
#     source = dff.brz.withColumn("index", F.row_number().over(w) - 1)
#     return source, source_path


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_drop(backend, node):
    df0 = get_df0(backend)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="x1 < 3",
            action="DROP",
        )
    ]
    node.execute()
    o = node.output_df.collect().to_pandas()

    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert len(o) == 2


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_quarantine(backend, node):
    df0 = get_df0(backend)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="x1 < 3",
            action="QUARANTINE",
        )
    ]
    node.execute()
    o = node.output_df.collect().to_pandas()
    q = node.quarantine_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert len(o) == 2
    assert len(q) == 1
    assert o["x1"].max() < 3
    assert q["x1"].min() >= 3


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_fail(backend, node):
    df0 = get_df0(backend)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="x1 < 3",
            action="FAIL",
        )
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_aggregate(backend, node):
    df0 = get_df0(backend)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="nw.max('x1') > 5",
            action="FAIL",
            type="AGGREGATE",
        )
    ]
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_multi(backend, node):
    df0 = get_df0(backend)
    # df0 = df0.with_columns(y1=nw.col("x1") - 1)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="max price",
            expr="x1 < 3",
            action="FAIL",
            type="QUARANTINE",
        )
    ]
    node.expectations = [
        models.DataQualityExpectation(
            name="x1_1",
            expr="x1 < 3",
            action="QUARANTINE",
        ),
        models.DataQualityExpectation(
            name="x1_2 amazon",
            expr="x1 >= 2",
            action="DROP",
        ),
    ]
    node.execute()
    o = node.output_df.collect().to_pandas()
    q = node.quarantine_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 3
    assert node.checks[1].fails_count == 1
    assert len(o) == 1
    assert len(q) == 1
    assert o["x1"].max() < 3
    assert q["x1"].min() >= 3


# def test_streaming_multi():
#     node_path = (
#         testdir_path / "tmp" / "test_pipeline_node_expectations" / str(uuid.uuid4())
#     )
#     checkpoint_path = node_path / "_checkpoint_expectations"
#
#     # Cleanup
#     if os.path.exists(str(node_path)):
#         shutil.rmtree(str(node_path))
#
#     # Create Stream Source
#     source, source_path = get_source(node_path)
#
#     # Insert a single row
#     source.filter("index=0").write.format("delta").mode("OVERWRITE").save(source_path)
#
#     # Get Node
#     node = get_node()
#     node.source = models.FileDataSource(
#         path=source_path,
#         format="DELTA",
#         as_stream=True,
#     )
#     node.expectations = [
#         models.DataQualityExpectation(
#             name="max price",
#             expr="close < 300",
#             action="WARN",
#         ),
#         models.DataQualityExpectation(
#             name="max price",
#             expr="close < 330",
#             action="QUARANTINE",
#         ),
#         models.DataQualityExpectation(
#             name="not amazon",
#             expr="symbol != 'AMZN'",
#             action="DROP",
#         ),
#     ]
#     node.expectations_checkpoint_location = checkpoint_path
#
#     # Execute
#     node.execute(spark=spark)
#
#     # Test
#     assert node.checks[0].status == "PASS"
#     assert node.checks[0].rows_count == 1
#     assert node.checks[0].fails_count == 0
#     assert node.checks[1].status == "PASS"
#     assert node.checks[1].rows_count == 1
#     assert node.checks[1].fails_count == 0
#     assert node.checks[2].status == "PASS"
#     assert node.checks[2].rows_count == 1
#     assert node.checks[2].fails_count == 0
#
#     # Update source
#     source.filter("index>0 AND index<40").write.format("delta").mode("append").save(
#         source_path
#     )
#     node.execute(spark=spark)
#     print(node.checks)
#     assert node.checks[0].status == "PASS"
#     assert node.checks[0].rows_count == 39
#     assert node.checks[0].fails_count == 0
#     assert node.checks[1].status == "PASS"
#     assert node.checks[1].rows_count == 39
#     assert node.checks[1].fails_count == 0
#     assert node.checks[2].status == "FAIL"
#     assert node.checks[2].rows_count == 39
#     assert node.checks[2].fails_count == 20
#
#     source.filter("index>=40").write.format("delta").mode("append").save(source_path)
#     node.execute(spark=spark)
#     print(node.checks)
#     assert node.checks[0].status == "FAIL"
#     assert node.checks[0].rows_count == 40
#     assert node.checks[0].fails_count == 20
#     assert node.checks[1].status == "FAIL"
#     assert node.checks[1].rows_count == 40
#     assert node.checks[1].fails_count == 8
#     assert node.checks[2].status == "PASS"
#     assert node.checks[2].rows_count == 40
#     assert node.checks[2].fails_count == 0
#
#     # Raise Exception
#     source.filter("index>=40").write.format("delta").mode("append").save(source_path)
#     node.expectations[0].action = "FAIL"
#     # node.execute(spark=spark)
#     # # TODO: Find how to capture exception
#     # # with pytest.raises(Exception):
#     # #     node.execute(spark=spark)
#
#
# def test_expectations_invalid():
#     with pytest.raises(DataQualityExpectationsNotSupported):
#         models.PipelineNode(
#             name="slv_stock_prices",
#             source={
#                 "path": "some_path",
#                 "format": "DELTA",
#                 "as_stream": True,
#             },
#             expectations=[
#                 {
#                     "name": "max price pass",
#                     "expr": "close < 300",
#                     "action": "WARN",
#                 },
#                 {
#                     "name": "max price drop",
#                     "expr": "count(*) > 20",
#                     "type": "AGGREGATE",
#                 },
#             ],
#             expectations_checkpoint_location="some_path",
#         )
#
#     with pytest.raises(DataQualityExpectationsNotSupported):
#         models.PipelineNode(
#             name="slv_stock_prices",
#             source={
#                 "path": "some_path",
#                 "format": "DELTA",
#                 "as_stream": True,
#             },
#             expectations=[
#                 {
#                     "name": "max price pass",
#                     "expr": "close < 300",
#                     "action": "WARN",
#                     "tolerance": {"abs": 20},
#                 },
#             ],
#             expectations_checkpoint_location="some_path",
#         )
#
#     # with pytest.warns(UserWarning):
#     #     node = models.PipelineNode(
#     #         name="slv_stock_prices",
#     #         source={
#     #             "path": "some_path",
#     #             "format": "DELTA",
#     #             "as_stream": True,
#     #         },
#     #         expectations=[
#     #             {
#     #                 "name": "max price pass",
#     #                 "expr": "F.('close') < 300",
#     #             },
#     #         ],
#     #     )
