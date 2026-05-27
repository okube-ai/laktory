"""Tests for DataFrameTransformer within PipelineNode: node refs, SQL placeholders, upstream names."""

import pytest
from pydantic import ValidationError

from laktory import models
from laktory._testing import StreamingSource


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_node_ref_in_func_args(backend, tmp_path):
    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "src")
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
        source={"format": "JSON", "path": src_path},
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    slv = models.PipelineNode(
        name="slv",
        source={"node_name": "brz"},
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
    assert sorted(df.columns) == ["_batch_id", "_idx", "id", "x1", "y1"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_node_placeholder(backend, tmp_path):
    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "src")
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
        source={"format": "JSON", "path": src_path},
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    slv = models.PipelineNode(
        name="slv",
        source=None,
        transformer={"nodes": [{"expr": "SELECT x1 FROM {nodes.brz}"}]},
        sinks=[{"format": "PARQUET", "path": slv_path, "mode": mode}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)
    pl.execute()

    df = pl.nodes_dict["slv"].primary_sink.read()
    assert df.columns == ["x1"]


def test_upstream_node_names():
    brz = models.PipelineNode(
        name="brz",
        sinks=[{"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}],
    )
    slv = models.PipelineNode(
        name="slv",
        source={"node_name": "brz"},
        sinks=[{"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}],
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv])
    assert pl.nodes_dict["brz"].upstream_node_names == []
    assert pl.nodes_dict["slv"].upstream_node_names == ["brz"]


def test_source_sink_validation_errors():
    with pytest.raises(ValidationError, match="FileDataSource"):
        models.PipelineNode.model_validate(
            {
                "name": "test",
                "source": {
                    "type": "FILE",
                    "path": "/data",
                    "format": "BADFORMAT",
                    "dataframe_backend": "POLARS",
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
