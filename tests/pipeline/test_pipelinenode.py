from pathlib import Path

import pytest

from laktory import get_spark_session
from laktory import models
from laktory._testing import StreamingSource
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.enums import STREAMING_BACKENDS
from laktory.enums import DataFrameBackends


def test_parents():
    df0 = get_df0("POLARS")

    node = models.PipelineNode(
        name="node0",
        source={
            "df": df0,
        },
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[
            {
                "path": "/",
                "format": "PARQUET",
            }
        ],
    )

    assert node.source.parent == node
    assert node.transformer.parent == node
    for n in node.transformer.nodes:
        assert n.parent == node.transformer
    for s in node.sinks:
        assert s.parent == node


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_execute(backend, tmp_path):
    df0 = get_df0(backend)

    mode = None
    sink_path = tmp_path / "pl_node_sink"
    if backend == "PYSPARK":
        mode = "OVERWRITE"
        sink_path = tmp_path / "pl_node_sink/"

    node = models.PipelineNode(
        name="node0",
        source={
            "df": df0,
        },
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[
            {
                "path": sink_path,
                "format": "PARQUET",
                "mode": mode,
            }
        ],
    )
    node.execute()
    df1 = node.primary_sink.read()

    assert df1.columns == ["id", "x1", "y1"]
    assert df1.collect().shape == (3, 3)

    # Test Full Refresh
    node.execute(full_refresh=True)

    # Test purge
    node.purge()
    for s in node.sinks:
        path = Path(s.path)
        assert not path.exists()


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_execute_stream(backend, tmp_path):
    if DataFrameBackends(backend) not in STREAMING_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    ss = StreamingSource(backend)
    source_path = str(tmp_path / "source")
    sink_path = str(tmp_path / "sink")

    node = models.PipelineNode(
        name="node0",
        source={"path": source_path, "format": "DELTA", "as_stream": "True"},
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                {"expr": "select id, x1, y1 from {df}"},
            ]
        },
        sinks=[
            {
                "path": sink_path,
                "format": "DELTA",
                "mode": "APPEND",
            }
        ],
    )

    ss.write_to_delta(source_path)
    df = node.execute()
    df1 = node.primary_sink.read()

    assert df.to_native().isStreaming
    assert df1.columns == ["id", "x1", "y1"]
    assert df1.collect().shape == (3, 3)

    # Test Full Refresh
    node.execute(full_refresh=True)

    # Test purge
    node.purge()
    if node.expectations_checkpoint_path:
        path = Path(node.expectations_checkpoint_path)
        assert not path.exists()
    for s in node.sinks:
        path = Path(s.path)
        assert not path.exists()


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_execute_view(backend, tmp_path):
    if backend == "POLARS":
        pytest.skip("Backend not supported. Skipping Test.")

    df0 = get_df0(backend)

    # Create table
    table_path = tmp_path / "df0/"
    (
        df0.to_native()
        .write.mode("OVERWRITE")
        .option("path", table_path)
        .saveAsTable("default.df0")
    )

    node = models.PipelineNode(
        name="node0",
        source={
            "schema_name": "default",
            "table_name": "df0",
        },
        sinks=[
            {
                "schema_name": "default",
                "table_name": "df1",
                "table_type": "VIEW",
                "view_definition": "SELECT id FROM {df}",
            }
        ],
    )

    df1_output = node.execute()
    df1_read = get_spark_session().read.table("default.df1")

    assert_dfs_equal(df1_output, df1_read)


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_purge_multisinks(backend, tmp_path):
    df0 = get_df0(backend)

    # Create table
    table_path = tmp_path / "df0/"
    (
        df0.to_native()
        .write.mode("OVERWRITE")
        .option("path", table_path)
        .saveAsTable("default.df0")
    )

    node = models.PipelineNode(
        name="node0",
        source={
            "schema_name": "default",
            "table_name": "df0",
        },
        sinks=[
            {
                "schema_name": "default",
                "table_name": "df1",
                "table_type": "VIEW",
                "view_definition": "SELECT id FROM {df}",
            },
            {
                "schema_name": "default",
                "table_name": "df2",
                "table_type": "VIEW",
                "view_definition": "SELECT id FROM {df}",
            },
        ],
    )

    node.purge()
