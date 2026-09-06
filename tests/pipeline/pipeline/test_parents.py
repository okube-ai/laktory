"""Tests for parent-reference propagation across Pipeline, PipelineNode, sources, sinks, and transformers."""

import pytest

from laktory import models
from laktory._testing import get_df0

_SINK = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}


def _get_pl():
    df0 = get_df0("POLARS")
    return models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                sources=[{"df": df0}],
                transformer={
                    "nodes": [
                        {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                        {"expr": "select id, x1, y1 from {df}"},
                    ]
                },
                sinks=[_SINK],
            ),
            models.PipelineNode(
                name="slv",
                sources=[{"node_name": "brz"}],
                sinks=[_SINK],
            ),
        ],
    )


def test_node_parent():
    pl = _get_pl()
    for node in pl.nodes:
        assert node.parent == pl
        assert node.parent_pipeline == pl


def test_source_parent():
    pl = _get_pl()
    for node in pl.nodes:
        for src in node.sources:
            assert src.parent == node
            assert src.parent_pipeline_node == node
            assert src.parent_pipeline == pl


def test_sink_parent():
    pl = _get_pl()
    for node in pl.nodes:
        for sink in node.all_sinks:
            assert sink.parent == node


def test_transformer_parent():
    pl = _get_pl()
    brz = pl.nodes_dict["brz"]
    assert brz.transformer.parent == brz
    assert brz.transformer.parent_pipeline == pl
    for tn in brz.transformer.nodes:
        assert tn.parent == brz.transformer
        assert tn.parent_pipeline == pl
        assert tn.parent_pipeline_node == brz


def test_parents_after_inject_vars():
    # `Pipeline.inject_vars(inplace=False)` (the default, used by
    # `laktory.dab.build_resources`) deep-copies the tree then resolves
    # variables into the copy. Nodes (one level deep) got correctly
    # re-parented, but grandchildren (sinks, transformer nodes - two levels
    # deep) kept a `_parent` pointing at the stale, pre-copy node instead of
    # the new one - see issue #653.
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                sources=[{"path": "brz.json", "format": "JSON"}],
                transformer={"nodes": [{"expr": "select * from {df}"}]},
                sinks=[_SINK],
            ),
        ],
    )
    pl2 = pl.inject_vars()

    node2 = pl2.nodes_dict["brz"]
    sink2 = node2.sinks[0]
    tn2 = node2.transformer.nodes[0]

    assert node2.parent_pipeline is pl2
    assert sink2.parent_pipeline_node is node2
    assert tn2.parent_pipeline_node is node2


def test_orchestrator_parent():
    pl = models.Pipeline(
        name="pl",
        nodes=[],
        orchestrator={
            "type": "LAKEFLOW_JOB",
            "name": "pl",
            "job_clusters": [
                {
                    "job_cluster_key": "node-cluster",
                    "new_cluster": {
                        "node_type_id": "Standard_DS3_v2",
                        "spark_version": "16.3.x-scala2.12",
                    },
                }
            ],
        },
    )
    assert pl.orchestrator.parent == pl
    assert pl.orchestrator.parent_pipeline == pl


def test_source_backward_compat():
    """Legacy `source:` key is auto-migrated to `sources: [...]`."""
    df0 = get_df0("POLARS")
    node = models.PipelineNode(
        name="brz",
        source={"df": df0},  # old API - must still work
    )
    assert len(node.sources) == 1
    assert node.sources[0].df is not None


def test_bad_node_name_raises():
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(name="a", sources=[{"node_name": "nonexistent"}]),
        ],
    )
    with pytest.raises(ValueError, match="nonexistent"):
        _ = pl.dag
