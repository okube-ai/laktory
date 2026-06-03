"""Tests for DataQualityExpectation on PipelineNode - WARN/DROP/QUARANTINE/FAIL, AGGREGATE, streaming, tolerance."""

import pytest

from laktory import models
from laktory._testing import StreamingSource
from laktory._testing import get_df0
from laktory.exceptions import DataQualityCheckFailedError
from laktory.exceptions import DataQualityExpectationsNotSupported


def _node_with(action, expr="x1 < 3", backend="POLARS", lazy=True):
    df0 = get_df0(backend, lazy=lazy)
    return models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        expectations=[
            models.DataQualityExpectation(name="check", expr=expr, action=action)
        ],
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_warn(backend):
    node = _node_with("WARN", backend=backend)
    node.execute()
    o = node.output_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert len(o) == 3  # WARN keeps all rows


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_drop(backend):
    node = _node_with("DROP", backend=backend)
    node.execute()
    o = node.output_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert len(o) == 2  # DROP removes failing row


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_quarantine(backend):
    node = _node_with("QUARANTINE", backend=backend)
    node.execute()
    o = node.output_df.collect().to_pandas()
    q = node.quarantine_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].fails_count == 1
    assert len(o) == 2
    assert len(q) == 1
    assert o["x1"].max() < 3
    assert q["x1"].min() >= 3


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_fail(backend):
    node = _node_with("FAIL", backend=backend)
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_aggregate_pass(backend):
    df0 = get_df0(backend, lazy=True)
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        expectations=[
            models.DataQualityExpectation(
                name="max ok",
                expr="nw.max('x1') <= 10",
                action="WARN",
                type="AGGREGATE",
            )
        ],
    )
    node.execute()
    assert node.checks[0].status == "PASS"
    assert node.checks[0].fails_count is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_aggregate_fail_raises(backend):
    df0 = get_df0(backend, lazy=True)
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        expectations=[
            models.DataQualityExpectation(
                name="max too big",
                expr="nw.max('x1') > 5",
                action="FAIL",
                type="AGGREGATE",
            )
        ],
    )
    with pytest.raises(DataQualityCheckFailedError):
        node.execute()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].fails_count is None


def test_aggregate_on_stream_raises():
    with pytest.raises(DataQualityExpectationsNotSupported):
        models.PipelineNode(
            name="node0",
            sources=[{"path": "some_path", "format": "DELTA", "as_stream": True}],
            expectations=[
                {"name": "row count ok", "expr": "count(*) > 20", "type": "AGGREGATE"},
            ],
            expectations_checkpoint_path="some_path",
        )


def test_tolerance_on_stream_raises():
    with pytest.raises(DataQualityExpectationsNotSupported):
        models.PipelineNode(
            name="node0",
            sources=[{"path": "some_path", "format": "DELTA", "as_stream": True}],
            expectations=[
                {
                    "name": "close ok",
                    "expr": "close < 300",
                    "action": "WARN",
                    "tolerance": {"abs": 20},
                },
            ],
            expectations_checkpoint_path="some_path",
        )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_multi_expectations(backend):
    df0 = get_df0(backend, lazy=True)
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        expectations=[
            models.DataQualityExpectation(
                name="quarantine x>=3", expr="x1 < 3", action="QUARANTINE"
            ),
            models.DataQualityExpectation(
                name="drop x<2", expr="x1 >= 2", action="DROP"
            ),
        ],
    )
    node.execute()
    o = node.output_df.collect().to_pandas()
    q = node.quarantine_df.collect().to_pandas()
    assert node.checks[0].status == "FAIL"
    assert node.checks[0].fails_count == 1
    assert node.checks[1].status == "FAIL"
    assert node.checks[1].fails_count == 1
    assert len(o) == 1
    assert len(q) == 1


def test_checks_populated():
    df0 = get_df0("POLARS", lazy=True)
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        expectations=[
            models.DataQualityExpectation(name="check", expr="x1 < 3", action="WARN")
        ],
    )
    node.execute()
    checks = node.checks
    assert len(checks) == 1
    assert checks[0].status in ("PASS", "FAIL")
    assert checks[0].rows_count == 3


def test_streaming_multi(tmp_path):
    ss = StreamingSource(backend="PYSPARK")
    source_path = str(tmp_path / "source")
    checkpoint_path = tmp_path / "node" / "_checkpoint"

    node = models.PipelineNode(
        name="node0",
        sources=[{"path": source_path, "format": "DELTA", "as_stream": True}],
        expectations_checkpoint_path_=checkpoint_path,
        expectations=[
            models.DataQualityExpectation(name="id_b", expr="id != 'b'", action="WARN"),
            models.DataQualityExpectation(
                name="batch2", expr="_batch_id != 2", action="QUARANTINE"
            ),
            models.DataQualityExpectation(name="drop3", expr="x1 != 3", action="DROP"),
        ],
    )

    ss.write_to_delta(source_path)
    node.execute()

    assert node.checks[0].status == "FAIL"
    assert node.checks[0].rows_count == 3
    assert node.checks[0].fails_count == 1
    assert node.checks[1].status == "PASS"
    assert node.checks[2].status == "FAIL"
    assert node.checks[2].rows_count == 3
    assert node.checks[2].fails_count == 1
