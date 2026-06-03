"""Tests for PipelineExecutionPlan - node selection, task grouping, and task DAG."""

import pytest

from laktory import models

_SINK = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}


@pytest.fixture
def pl():
    return models.Pipeline(
        name="test",
        nodes=[
            models.PipelineNode(name="brz_a", tags=["brz", "a"], sinks=[_SINK]),
            models.PipelineNode(
                name="slv_a1",
                execution_task_name="slv_a",
                sources=[{"node_name": "brz_a"}],
                tags=["slv", "a"],
                sinks=[_SINK],
            ),
            models.PipelineNode(
                name="slv_a2",
                execution_task_name="slv_a",
                sources=[{"node_name": "brz_a"}],
                tags=["slv", "a"],
                sinks=[_SINK],
            ),
            models.PipelineNode(
                name="gld_a",
                sources=[{"node_name": "slv_a1"}],
                tags=["gld", "a"],
                sinks=[_SINK],
            ),
            models.PipelineNode(name="brz_b", tags=["brz", "b"], sinks=[_SINK]),
            models.PipelineNode(
                name="slv_b1",
                execution_task_name="slv_b",
                sources=[{"node_name": "slv_b2"}],
                tags=["slv", "b"],
                sinks=[_SINK],
            ),
            models.PipelineNode(
                name="slv_b2",
                execution_task_name="slv_b",
                sources=[{"node_name": "brz_b"}],
                tags=["slv", "b"],
                sinks=[_SINK],
            ),
            models.PipelineNode(
                name="gld_b",
                sources=[{"node_name": "slv_b1"}],
                tags=["gld", "b"],
                sinks=[_SINK],
            ),
        ],
    )


@pytest.fixture
def pl_with_views():
    return models.Pipeline(
        name="test",
        nodes=[
            models.PipelineNode(name="brz", sinks=[_SINK]),
            models.PipelineNode(name="slv", sources=[{"node_name": "brz"}]),
            models.PipelineNode(
                name="gld1", sources=[{"node_name": "slv"}], sinks=[_SINK]
            ),
            models.PipelineNode(name="gld2", sources=[{"node_name": "slv"}]),
        ],
    )


# --------------------------------------------------------------------------- #
# Node selection                                                              #
# --------------------------------------------------------------------------- #


def test_no_selects_all_nodes(pl):
    plan = pl.get_execution_plan()
    assert plan.node_names == [
        "brz_a",
        "brz_b",
        "slv_a1",
        "slv_a2",
        "slv_b2",
        "gld_a",
        "slv_b1",
        "gld_b",
    ]


def test_select_single_node(pl):
    plan = pl.get_execution_plan(selects=["slv_a1"])
    assert plan.node_names == ["slv_a1"]


def test_select_by_execution_task_name(pl):
    plan = pl.get_execution_plan(selects=["slv_a"])
    assert plan.node_names == ["slv_a1", "slv_a2"]

    plan = pl.get_execution_plan(selects=["slv_b"])
    assert plan.node_names == ["slv_b2", "slv_b1"]


def test_select_by_tag(pl):
    plan = pl.get_execution_plan(selects=["gld"])
    assert plan.node_names == ["gld_a", "gld_b"]


def test_select_upstream(pl):
    plan = pl.get_execution_plan(selects=["*slv_a1"])
    assert plan.node_names == ["brz_a", "slv_a1"]


def test_select_downstream(pl):
    plan = pl.get_execution_plan(selects=["slv_a1*"])
    assert plan.node_names == ["slv_a1", "gld_a"]


def test_select_both_directions(pl):
    plan = pl.get_execution_plan(selects=["*slv_a1*"])
    assert plan.node_names == ["brz_a", "slv_a1", "gld_a"]


def test_select_both_directions_group(pl):
    plan = pl.get_execution_plan(selects=["*slv_a*"])
    assert plan.node_names == ["brz_a", "slv_a1", "slv_a2", "gld_a"]


def test_wildcard_upstream_tag(pl):
    plan = pl.get_execution_plan(selects=["*gld"])
    assert plan.node_names == [
        "brz_a",
        "brz_b",
        "slv_a1",
        "slv_b2",
        "gld_a",
        "slv_b1",
        "gld_b",
    ]


# --------------------------------------------------------------------------- #
# Task grouping                                                               #
# --------------------------------------------------------------------------- #


def test_task_grouping(pl):
    plan = models.PipelineExecutionPlan(pipeline=pl)
    tasks = {task.name: task.node_names for task in plan.tasks}
    assert tasks == {
        "node-brz_a": ["brz_a"],
        "node-brz_b": ["brz_b"],
        "slv_a": ["slv_a1", "slv_a2"],
        "slv_b": ["slv_b2", "slv_b1"],
        "node-gld_a": ["gld_a"],
        "node-gld_b": ["gld_b"],
    }


# --------------------------------------------------------------------------- #
# Task DAG edges                                                              #
# --------------------------------------------------------------------------- #


def test_task_dag_edges(pl):
    plan = models.PipelineExecutionPlan(pipeline=pl)
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert upstreams == {
        "node-brz_a": [],
        "node-brz_b": [],
        "slv_a": ["node-brz_a"],
        "slv_b": ["node-brz_b"],
        "node-gld_a": ["slv_a"],
        "node-gld_b": ["slv_b"],
    }


def test_task_dag_edges_with_selects(pl):
    plan = pl.get_execution_plan(selects=["slv_a", "slv_b"])
    tasks = {task.name: task.node_names for task in plan.tasks}
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert tasks == {"slv_a": ["slv_a1", "slv_a2"], "slv_b": ["slv_b2", "slv_b1"]}
    assert upstreams == {"slv_a": [], "slv_b": []}


def test_task_dag_partial_downstream(pl):
    plan = pl.get_execution_plan(selects=["slv_a1*"])
    tasks = {task.name: task.node_names for task in plan.tasks}
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert tasks == {"slv_a": ["slv_a1"], "node-gld_a": ["gld_a"]}
    assert upstreams == {"slv_a": [], "node-gld_a": ["slv_a"]}


# --------------------------------------------------------------------------- #
# Views plan - nodes without sinks are excluded from tasks                   #
# --------------------------------------------------------------------------- #


def test_views_plan_dag(pl_with_views):
    plan = models.PipelineExecutionPlan(pipeline=pl_with_views)
    tasks = {task.name: task.node_names for task in plan.tasks}
    assert tasks == {"node-brz": ["brz"], "node-gld1": ["gld1"], "node-gld2": ["gld2"]}


def test_unknown_node_raises(pl):
    plan = pl.get_execution_plan(selects=["nonexistent"])
    with pytest.raises(ValueError, match="nonexistent"):
        _ = plan.node_names
