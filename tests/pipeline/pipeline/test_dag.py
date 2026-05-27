"""Tests for Pipeline DAG construction and topological ordering."""

import networkx as nx
import pytest

from laktory import models

_SINK = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}


def _linear_pl():
    return models.Pipeline(
        name="pl-linear",
        nodes=[
            models.PipelineNode(name="brz", sinks=[_SINK]),
            models.PipelineNode(name="slv", source={"node_name": "brz"}, sinks=[_SINK]),
            models.PipelineNode(name="gld", source={"node_name": "slv"}, sinks=[_SINK]),
        ],
    )


def _diamond_pl():
    return models.Pipeline(
        name="pl-diamond",
        nodes=[
            models.PipelineNode(name="brz", sinks=[_SINK]),
            models.PipelineNode(
                name="slv1", source={"node_name": "brz"}, sinks=[_SINK]
            ),
            models.PipelineNode(
                name="slv2", source={"node_name": "brz"}, sinks=[_SINK]
            ),
            models.PipelineNode(
                name="gld",
                source={"node_name": "slv1"},
                transformer={"nodes": [{"expr": "SELECT * from {nodes.slv2}"}]},
                sinks=[_SINK],
            ),
        ],
    )


def test_linear_dag():
    pl = _linear_pl()
    dag = pl.dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 3
    assert len(dag.edges) == 2
    assert list(nx.topological_sort(dag)) == ["brz", "slv", "gld"]


def test_diamond_dag():
    pl = _diamond_pl()
    dag = pl.dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 4
    sorted_names = list(nx.topological_sort(dag))
    assert sorted_names.index("brz") < sorted_names.index("slv1")
    assert sorted_names.index("brz") < sorted_names.index("slv2")
    assert sorted_names.index("slv1") < sorted_names.index("gld")
    assert sorted_names.index("slv2") < sorted_names.index("gld")


def test_sorted_nodes_order():
    pl = _linear_pl()
    assert [n.name for n in pl.sorted_nodes] == ["brz", "slv", "gld"]


def test_nodes_dict():
    pl = _linear_pl()
    nd = pl.nodes_dict
    assert set(nd.keys()) == {"brz", "slv", "gld"}
    assert nd["brz"] is pl.nodes[0]
    assert nd["slv"] is pl.nodes[1]
    assert nd["gld"] is pl.nodes[2]


def test_upstream_node_names():
    pl = _linear_pl()
    nd = pl.nodes_dict
    assert nd["brz"].upstream_node_names == []
    assert nd["slv"].upstream_node_names == ["brz"]
    assert nd["gld"].upstream_node_names == ["slv"]


def test_no_source_node():
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(name="brz", sinks=[_SINK]),
            models.PipelineNode(
                name="gld",
                transformer={"nodes": [{"expr": "SELECT * from {nodes.brz}"}]},
                sinks=[_SINK],
            ),
        ],
    )
    dag = pl.dag
    assert "brz" in dag.nodes
    assert "gld" in dag.nodes
    assert "brz" in list(dag.predecessors("gld"))


def test_circular_dep_raises():
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(name="a", source={"node_name": "b"}),
            models.PipelineNode(name="b", source={"node_name": "a"}),
        ],
    )
    with pytest.raises(ValueError, match="circular"):
        _ = pl.dag
