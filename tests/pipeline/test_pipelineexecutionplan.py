from pathlib import Path

import pytest

from laktory import models

data_dirpath = Path(__file__).parent.parent / "data"

OPEN_FIGURES = False


@pytest.fixture
def pl():
    sink = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}

    pl = models.Pipeline(
        name="test",
        nodes=[
            models.PipelineNode(
                name="brz_a",
                tags=["brz", "a"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="slv_a1",
                execution_task_name="slv_a",
                source={"node_name": "brz_a"},
                tags=["slv", "a"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="slv_a2",
                execution_task_name="slv_a",
                source={"node_name": "brz_a"},
                tags=["slv", "a"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="gld_a",
                source={"node_name": "slv_a1"},
                tags=["gld", "a"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="brz_b",
                tags=["brz", "b"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="slv_b1",
                execution_task_name="slv_b",
                source={"node_name": "slv_b2"},
                tags=["slv", "b"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="slv_b2",
                execution_task_name="slv_b",
                source={"node_name": "brz_b"},
                tags=["slv", "b"],
                sinks=[sink],
            ),
            models.PipelineNode(
                name="gld_b",
                source={"node_name": "slv_b1"},
                tags=["gld", "b"],
                sinks=[sink],
            ),
        ],
    )
    return pl


@pytest.fixture
def pl_with_views():
    sink = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}

    pl = models.Pipeline(
        name="test",
        nodes=[
            models.PipelineNode(
                name="brz",
                sinks=[sink],
            ),
            models.PipelineNode(
                name="slv",
                source={"node_name": "brz"},
            ),
            models.PipelineNode(
                name="gld1",
                source={"node_name": "slv"},
                sinks=[sink],
            ),
            models.PipelineNode(
                name="gld2",
                source={"node_name": "slv"},
            ),
        ],
    )
    return pl


def test_selected_nodes(pl):
    # All nodes
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

    # Single Nodes
    plan = pl.get_execution_plan(selects=["slv_a1"])
    assert plan.node_names == ["slv_a1"]

    # Single Group
    plan = pl.get_execution_plan(selects=["slv_a"])
    assert plan.node_names == ["slv_a1", "slv_a2"]

    # Single Group
    plan = pl.get_execution_plan(selects=["slv_b"])
    assert plan.node_names == ["slv_b2", "slv_b1"]

    # Single Tag
    plan = pl.get_execution_plan(selects=["gld"])
    assert plan.node_names == ["gld_a", "gld_b"]

    # Upstream
    plan = pl.get_execution_plan(selects=["*slv_a1"])
    assert plan.node_names == ["brz_a", "slv_a1"]

    # Downstream
    plan = pl.get_execution_plan(selects=["slv_a1*"])
    assert plan.node_names == ["slv_a1", "gld_a"]

    # Downstream and Upstream
    plan = pl.get_execution_plan(selects=["*slv_a1*"])
    assert plan.node_names == ["brz_a", "slv_a1", "gld_a"]

    # Downstream and Upstream Group
    plan = pl.get_execution_plan(selects=["*slv_a*"])
    assert plan.node_names == ["brz_a", "slv_a1", "slv_a2", "gld_a"]

    # Downstream tag
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


def test_plan_dag(pl):
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
    )
    tasks = {task.name: task.node_names for task in plan.tasks}
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert tasks == {
        "node-brz_a": ["brz_a"],
        "node-brz_b": ["brz_b"],
        "slv_a": ["slv_a1", "slv_a2"],
        "slv_b": ["slv_b2", "slv_b1"],
        "node-gld_a": ["gld_a"],
        "node-gld_b": ["gld_b"],
    }
    assert upstreams == {
        "node-brz_a": [],
        "node-brz_b": [],
        "slv_a": ["node-brz_a"],
        "slv_b": ["node-brz_b"],
        "node-gld_a": ["slv_a"],
        "node-gld_b": ["slv_b"],
    }

    plan = pl.get_execution_plan(selects=["slv_a", "slv_b"])
    tasks = {task.name: task.node_names for task in plan.tasks}
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert tasks == {
        "slv_a": ["slv_a1", "slv_a2"],
        "slv_b": ["slv_b2", "slv_b1"],
    }
    assert upstreams == {"slv_a": [], "slv_b": []}

    plan = pl.get_execution_plan(selects=["slv_a1*"])
    tasks = {task.name: task.node_names for task in plan.tasks}
    upstreams = {task.name: task.upstream_task_names for task in plan.tasks}
    assert tasks == {"slv_a": ["slv_a1"], "node-gld_a": ["gld_a"]}
    assert upstreams == {"slv_a": [], "node-gld_a": ["slv_a"]}


def test_views_plan_dag(pl_with_views):
    plan = models.PipelineExecutionPlan(
        pipeline=pl_with_views,
    )

    tasks = {task.name: task.node_names for task in plan.tasks}
    assert tasks == {"node-brz": ["brz"], "node-gld1": ["gld1"], "node-gld2": ["gld2"]}
