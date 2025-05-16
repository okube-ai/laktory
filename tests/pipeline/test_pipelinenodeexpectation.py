import pyspark.sql.functions as F
import pytest
from pyspark.sql import Window

from laktory import models
from laktory._testing import get_df0
from laktory.enums import STREAMING_BACKENDS
from laktory.enums import DataFrameBackends
from laktory.exceptions import DataQualityCheckFailedError
from laktory.exceptions import DataQualityExpectationsNotSupported


@pytest.fixture
def node():
    node = models.PipelineNode(
        name="node0",
        expectations=[
            models.DataQualityExpectation(
                name="max price",
                expr="x1 < 3",
                action="WARN",
            )
        ],
    )
    return node


def get_source_df(backend):
    df0 = get_df0(backend, lazy=True)
    source0 = df0.to_native()

    source = source0.withColumn("_batch_id", F.lit(0))
    for i in range(4):
        source = source.union(source0.withColumn("_batch_id", F.lit(i + 1)))

    w = Window.orderBy("_batch_id", "id")
    source = source.withColumn("_idx", F.row_number().over(w) - 1)

    return source


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_warn(backend, node):
    df0 = get_df0(backend, lazy=True)

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


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_drop(backend, node):
    df0 = get_df0(backend, lazy=True)
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
    df0 = get_df0(backend, lazy=True)
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
    df0 = get_df0(backend, lazy=True)
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
    df0 = get_df0(backend, lazy=True)
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
    df0 = get_df0(backend, lazy=True)
    node.source = models.DataFrameDataSource(df=df0)
    node.expectations = [
        models.DataQualityExpectation(
            name="x1_1",
            expr="x1 < 3",
            action="QUARANTINE",
        ),
        models.DataQualityExpectation(
            name="x1_2",
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


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_streaming_multi(backend, node, tmp_path):
    if DataFrameBackends(backend) not in STREAMING_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Set paths
    source_path = str(tmp_path / "source")
    node_path = tmp_path / "node"
    checkpoint_path = node_path / "_checkpoint_expectations"

    source = get_source_df(backend)

    # Initialize source
    source.filter(F.col("_idx") == 0).write.format("DELTA").mode("overwrite").save(
        source_path
    )
    # dfs = spark.readStream.format("DELTA").load(source_path)

    source.show()

    # Set node source
    node.source = models.FileDataSource(
        path=source_path,
        format="DELTA",
        as_stream=True,
    )
    node.expectations_checkpoint_path = checkpoint_path
    node.expectations = [
        models.DataQualityExpectation(
            name="id_b",
            expr="id != 'b'",
            action="WARN",
        ),
        models.DataQualityExpectation(
            name="batch2",
            expr="_batch_id != 2",
            action="QUARANTINE",
        ),
        models.DataQualityExpectation(
            name="drop3",
            expr="x1 != 3",
            action="DROP",
        ),
    ]

    # Execute
    node.execute()

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
    source.filter("_batch_id<2 AND _idx!=0").write.format("delta").mode("append").save(
        source_path
    )
    node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 5
    assert node.checks[0].fails_count == 2
    assert node.checks[1].status == "PASS"
    assert node.checks[1].rows_count == 5
    assert node.checks[1].fails_count == 0
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 5
    assert node.checks[2].fails_count == 2

    source.filter("_batch_id>=2").write.format("delta").mode("append").save(source_path)
    node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 9
    assert node.checks[0].fails_count == 3
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].rows_count == 9
    assert node.checks[1].fails_count == 3
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 9
    assert node.checks[2].fails_count == 3


def test_aggregate_on_stream():
    with pytest.raises(DataQualityExpectationsNotSupported):
        models.PipelineNode(
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
            expectations_checkpoint_path="some_path",
        )


def test_non_zero_tol():
    with pytest.raises(DataQualityExpectationsNotSupported):
        models.PipelineNode(
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
            expectations_checkpoint_path="some_path",
        )


# def test_non_zero_tol():
#     with pytest.warns(UserWarning):
#         node = models.PipelineNode(
#             name="slv_stock_prices",
#             source={
#                 "path": "some_path",
#                 "format": "DELTA",
#                 "as_stream": True,
#             },
#             expectations=[
#                 {
#                     "name": "max price pass",
#                     "expr": "F.('close') < 300",
#                 },
#             ],
#         )
