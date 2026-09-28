"""Tests for PipelineNode.execute() - batch, streaming, and chaining."""

from pathlib import Path

import narwhals as nw
import pytest

from laktory import models
from laktory._testing import StreamingSource
from laktory._testing import get_df0

from ...conftest import assert_dfs_equal


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_batch_execute(backend, tmp_path):
    df0 = get_df0(backend)
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    sink_path = str(tmp_path / "sink") + ("/" if backend == "PYSPARK" else "")

    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[{"path": sink_path, "format": "PARQUET", "mode": mode}],
    )
    node.execute()
    df1 = node.primary_sink.read()

    expected = get_df0(backend).with_columns(y1=nw.col("x1")).select(["id", "x1", "y1"])
    assert_dfs_equal(df1, expected)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_full_refresh(backend, tmp_path):
    df0 = get_df0(backend)
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    sink_path = str(tmp_path / "sink") + ("/" if backend == "PYSPARK" else "")

    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        sinks=[{"path": sink_path, "format": "PARQUET", "mode": mode}],
    )
    node.execute()
    node.execute(refresh="FULL")  # should not raise
    df1 = node.primary_sink.read()
    assert df1.collect().shape[0] == 3


def test_refresh_reset(tmp_path):
    """`refresh='RESET'` only resets the sinks, without reading or writing data"""
    df0 = get_df0("POLARS")
    sink_path = str(tmp_path / "sink")
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        sinks=[{"path": sink_path, "format": "DELTA", "mode": "APPEND"}],
    )
    node.execute()
    assert node.primary_sink.read().collect().shape[0] == 3

    node._output_df = None
    assert node.execute(refresh="reset") is None
    assert not Path(sink_path).exists()
    assert node.output_df is None

    # Override: TRUNCATE keeps the table
    node.execute()
    node.execute(refresh="RESET", reset_mode="TRUNCATE")
    assert Path(sink_path).exists()
    assert node.primary_sink.read().collect().shape[0] == 0


def test_refresh_parameters_validated(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"df": get_df0("POLARS")}],
        sinks=[{"path": str(tmp_path / "sink"), "format": "DELTA", "mode": "APPEND"}],
    )
    with pytest.raises(ValueError, match="is not supported"):
        node.execute(refresh="partial")
    with pytest.raises(ValueError, match="requires `refresh`"):
        node.execute(reset_mode="DROP")

    # Replaced by `refresh` in 0.13.0
    with pytest.raises(ValueError, match="run with `refresh='FULL'` instead"):
        node.execute(full_refresh=True)


def test_streaming_execute(tmp_path):
    ss = StreamingSource("PYSPARK")
    source_path = str(tmp_path / "source")
    sink_path = str(tmp_path / "sink")

    node = models.PipelineNode(
        name="node0",
        sources=[{"path": source_path, "format": "DELTA", "as_stream": "True"}],
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[{"path": sink_path, "format": "DELTA", "mode": "APPEND"}],
    )

    ss.write_to_delta(source_path)
    df = node.execute()
    df1 = node.primary_sink.read()

    assert df.to_native().isStreaming
    expected = (
        get_df0("PYSPARK").with_columns(y1=nw.col("x1")).select(["id", "x1", "y1"])
    )
    assert_dfs_equal(df1, expected)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_node_chaining(backend, tmp_path):
    ss = StreamingSource(backend)
    df0 = ss.write_to_json(tmp_path / "src")
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz") + ("/" if backend == "PYSPARK" else "")
    slv_path = str(tmp_path / "slv") + ("/" if backend == "PYSPARK" else "")
    # Polars reads a specific file; PySpark reads a directory
    src_path = (
        str(tmp_path / "src" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "src") + "/"
    )

    brz = models.PipelineNode(
        name="brz",
        sources=[{"format": "JSON", "path": src_path}],
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    slv = models.PipelineNode(
        name="slv",
        sources=[{"node_name": "brz"}],
        sinks=[{"format": "PARQUET", "path": slv_path, "mode": mode}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)
    pl.execute()

    df = pl.nodes_dict["slv"].primary_sink.read()
    assert_dfs_equal(df, df0)


def test_batch_append(tmp_path):
    """APPEND mode accumulates rows across node executions (PySpark only)."""
    sink_path = str(tmp_path / "sink") + "/"

    node = models.PipelineNode(
        name="node0",
        sources=[{"df": get_df0("PYSPARK")}],
        sinks=[{"path": sink_path, "format": "PARQUET", "mode": "APPEND"}],
    )

    node.execute()
    assert node.primary_sink.read().collect().shape[0] == 3

    node.execute()  # same source, APPEND: 3 more rows added
    assert node.primary_sink.read().collect().shape[0] == 6


def test_batch_delta(tmp_path):
    """Batch Delta read → transformer → Delta OVERWRITE write, values verified."""
    source_path = str(tmp_path / "source")
    sink_path = str(tmp_path / "sink")

    ss = StreamingSource("PYSPARK")
    ss.write_to_delta(source_path)

    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "DELTA", "path": source_path}],
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[{"path": sink_path, "format": "DELTA", "mode": "OVERWRITE"}],
    )
    node.execute()
    df1 = node.primary_sink.read()

    expected = (
        get_df0("PYSPARK").with_columns(y1=nw.col("x1")).select(["id", "x1", "y1"])
    )
    assert_dfs_equal(df1, expected)
