"""Tests for DataFrameTransformer within PipelineNode: node refs, SQL placeholders, upstream names."""

import narwhals as nw
import pytest
from pydantic import ValidationError

from laktory import models
from laktory._testing import StreamingSource

from ...conftest import assert_dfs_equal


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_node_ref_in_func_args(backend, tmp_path):
    ss = StreamingSource(backend)
    brz_df = ss.write_to_json(tmp_path / "src")
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz") + ("/" if backend == "PYSPARK" else "")
    slv_path = str(tmp_path / "slv") + ("/" if backend == "PYSPARK" else "")
    src_path = (
        str(tmp_path / "src" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "src") + "/"
    )

    brz = models.PipelineNode(
        name="brz",
        sources={"df": {"format": "JSON", "path": src_path}},
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    slv = models.PipelineNode(
        name="slv",
        sources={"df": {"node_name": "brz"}},
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
            ]
        },
        sinks=[{"format": "PARQUET", "path": slv_path, "mode": mode}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)
    pl.execute()

    df = pl.nodes_dict["slv"].primary_sink.read()
    assert_dfs_equal(df, brz_df.with_columns(y1=nw.col("x1")))


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_node_placeholder(backend, tmp_path):
    ss = StreamingSource(backend)
    brz_df = ss.write_to_json(tmp_path / "src")
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz") + ("/" if backend == "PYSPARK" else "")
    slv_path = str(tmp_path / "slv") + ("/" if backend == "PYSPARK" else "")
    src_path = (
        str(tmp_path / "src" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "src") + "/"
    )

    brz = models.PipelineNode(
        name="brz",
        sources={"df": {"format": "JSON", "path": src_path}},
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    slv = models.PipelineNode(
        name="slv",
        transformer={"nodes": [{"expr": "SELECT x1 FROM {nodes.brz}"}]},
        sinks=[{"format": "PARQUET", "path": slv_path, "mode": mode}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)
    pl.execute()

    df = pl.nodes_dict["slv"].primary_sink.read()
    assert_dfs_equal(df, brz_df.select("x1"))


def test_upstream_node_names():
    brz = models.PipelineNode(
        name="brz",
        sinks=[{"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}],
    )
    slv = models.PipelineNode(
        name="slv",
        sources={"df": {"node_name": "brz"}},
        sinks=[{"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv])
    assert pl.nodes_dict["brz"].upstream_node_names == []
    assert pl.nodes_dict["slv"].upstream_node_names == ["brz"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_multi_sources(backend):
    """Node with multiple named sources: each key accessible in SQL, {df} = first source."""

    from laktory._testing import get_df0
    from laktory._testing import get_df1

    df0 = get_df0(backend)
    df1 = get_df1(backend)

    node = models.PipelineNode(
        name="slv",
        sources={
            "events": {"df": df0},
            "ref": {"df": df1},
        },
        transformer={
            "nodes": [
                {
                    "expr": "SELECT {sources.events}.id, {sources.events}.x1, {sources.ref}.x2 FROM {sources.events} LEFT JOIN {sources.ref} ON {sources.events}.id = {sources.ref}.id"
                },
                # Second step: {df} is the flowing result, not the original source
                {"expr": "SELECT * FROM {df} WHERE id != 'a'"},
            ]
        },
    )

    assert not node.has_streaming_source
    assert node.upstream_node_names == []

    node.execute(write_sinks=False)
    result = node.output_df.collect().to_native()
    assert len(result) == 2  # 'b' and 'c' after filtering 'a'


def test_has_streaming_source():
    """has_streaming_source is True when any source is streaming."""
    node_static = models.PipelineNode(
        name="n",
        sources={"df": {"format": "PARQUET", "path": "/tmp/x"}},
    )
    node_streaming = models.PipelineNode(
        name="n",
        sources={"df": {"format": "PARQUET", "path": "/tmp/x", "as_stream": True}},
    )
    node_mixed = models.PipelineNode(
        name="n",
        sources={
            "a": {"format": "PARQUET", "path": "/tmp/a"},
            "b": {"format": "DELTA", "path": "/tmp/b", "as_stream": True},
        },
    )
    assert not node_static.has_streaming_source
    assert node_streaming.has_streaming_source
    assert node_mixed.has_streaming_source


def test_sourceless_node():
    """Node with no sources is valid; _stage_df starts as None."""
    node = models.PipelineNode(
        name="gen",
        transformer={
            "nodes": [
                {"func_name": "with_columns", "func_kwargs": {"x": "nw.lit(1)"}},
            ]
        },
    )
    assert node.sources == {}
    assert not node.has_streaming_source


def test_source_sink_validation_errors():
    with pytest.raises(ValidationError, match="FileDataSource"):
        models.PipelineNode.model_validate(
            {
                "name": "test",
                "sources": {
                    "df": {
                        "type": "FILE",
                        "path": "/data",
                        "format": "BADFORMAT",
                        "dataframe_backend": "POLARS",
                    }
                },
            }
        )

    with pytest.raises(ValidationError, match="FileDataSink"):
        models.PipelineNode.model_validate(
            {
                "name": "test",
                "sinks": [
                    {
                        "type": "FILE",
                        "path": "/data",
                        "format": "PARQUET",
                        "bad_field": "x",
                    }
                ],
            }
        )
