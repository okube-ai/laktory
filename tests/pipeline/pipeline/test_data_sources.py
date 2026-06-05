"""Tests for Pipeline.data_sources and PipelineNode.data_sources."""

from laktory import models
from laktory.models.datasources.filedatasource import FileDataSource
from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource


def _build_pipeline():
    """
    brz: FileDataSource("/tmp/raw")
    slv: explicit PipelineNodeDataSource("brz") + {nodes.brz} in SQL  → deduplicated to one
    gld: explicit PipelineNodeDataSource("slv") + {nodes.brz} via DataFrameMethod join arg
    """
    brz = models.PipelineNode(
        name="brz",
        sources=[{"path": "/tmp/raw", "format": "PARQUET"}],
    )

    slv = models.PipelineNode(
        name="slv",
        sources=[{"node_name": "brz"}],
        transformer={
            "nodes": [
                {
                    "type": "SQL",
                    "expr": "SELECT * FROM {df} UNION SELECT * FROM {nodes.brz}",
                }
            ]
        },
    )

    gld = models.PipelineNode(
        name="gld",
        sources=[{"node_name": "slv"}],
        transformer={
            "nodes": [
                {
                    "func_name": "join",
                    "func_args": ["{nodes.brz}"],
                    "func_kwargs": {"on": "id", "how": "left"},
                }
            ]
        },
    )

    return models.Pipeline(name="pl-test", nodes=[brz, slv, gld])


def test_brz_data_sources():
    pl = _build_pipeline()
    brz = pl.nodes_dict["brz"]
    assert len(brz.data_sources) == 1
    assert isinstance(brz.data_sources[0], FileDataSource)
    assert brz.data_sources[0].path == "/tmp/raw"


def test_slv_data_sources_deduplication():
    pl = _build_pipeline()
    slv = pl.nodes_dict["slv"]
    # brz appears as explicit source AND as {nodes.brz} in SQL — must be deduplicated
    assert len(slv.data_sources) == 1
    assert isinstance(slv.data_sources[0], PipelineNodeDataSource)
    assert slv.data_sources[0].node_name == "brz"


def test_gld_data_sources():
    pl = _build_pipeline()
    gld = pl.nodes_dict["gld"]
    node_names = {
        s.node_name for s in gld.data_sources if isinstance(s, PipelineNodeDataSource)
    }
    assert node_names == {"slv", "brz"}


def test_pipeline_data_sources():
    pl = _build_pipeline()
    sources = pl.data_sources

    file_sources = [s for s in sources if isinstance(s, FileDataSource)]
    node_sources = [s for s in sources if isinstance(s, PipelineNodeDataSource)]

    assert len(file_sources) == 1
    assert file_sources[0].path == "/tmp/raw"

    referenced_nodes = {s.node_name for s in node_sources}
    assert referenced_nodes == {"brz", "slv"}
