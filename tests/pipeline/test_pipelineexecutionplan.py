import io
import pytest
from pathlib import Path
from laktory import models

data_dirpath = Path(__file__).parent.parent / "data"

OPEN_FIGURES = False



@pytest.fixture
def pl():
    pl = models.Pipeline(
        name="test",
        nodes=[
            models.PipelineNode(
                name="brz_a",
                tags=["brz", "a"],
            ),
            models.PipelineNode(
                name="slv_a1",
                group="slv_a",
                source={"node_name": "brz_a"},
                tags=["slv", "a"],
            ),
            models.PipelineNode(
                name="slv_a2",
                group="slv_a",
                source={"node_name": "brz_a"},
                tags=["slv", "a"],
            ),
            models.PipelineNode(
                name="gld_a",
                source={"node_name": "slv_a1"},
                tags=["gld", "a"],
            ),
            models.PipelineNode(
                name="brz_b",
                tags=["brz", "b"],
            ),
            models.PipelineNode(
                name="slv_b1",
                group="slv_b",
                source={"node_name": "slv_b2"},
                tags=["slv", "b"],
            ),
            models.PipelineNode(
                name="slv_b2",
                group="slv_b",
                source={"node_name": "brz_b"},
                tags=["slv", "b"],
            ),
            models.PipelineNode(
                name="gld_b",
                source={"node_name": "slv_b1"},
                tags=["gld", "b"],
            ),
        ]
    )
    return pl


def test_selected_nodes(pl):

    # All nodes
    plan = models.PipelineExecutionPlan(
        pipeline=pl
    )
    assert plan.node_names == ['brz_a', 'brz_b', 'slv_a1', 'slv_a2', 'slv_b2', 'gld_a', 'slv_b1', 'gld_b']

    # Single Nodes
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_a1"]
    )
    assert plan.node_names == ['slv_a1']

    # Single Group
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_a"]
    )
    assert plan.node_names == ['slv_a1', 'slv_a2']

    # Single Group
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_b"]
    )
    assert plan.node_names == ['slv_b2', 'slv_b1']

    # Single Tag
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["gld"]
    )
    assert plan.node_names == ['gld_a', 'gld_b']

    # Upstream
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["*slv_a1"]
    )
    assert plan.node_names == ['brz_a', 'slv_a1']

    # Downstream
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_a1*"]
    )
    assert plan.node_names == ['slv_a1', 'gld_a']


    # Downstream and Upstream
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["*slv_a1*"]
    )
    assert plan.node_names == ["brz_a", 'slv_a1', 'gld_a']

    # Downstream and Upstream Group
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["*slv_a*"]
    )
    assert plan.node_names == ["brz_a", 'slv_a1', 'slv_a2', 'gld_a']

    # Downstream tag
    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["*gld"]
    )
    assert plan.node_names == ['brz_a', 'brz_b', 'slv_a1', 'slv_b2', 'gld_a', 'slv_b1', 'gld_b']


def test_plan_dag(pl):

    plan = models.PipelineExecutionPlan(
        pipeline=pl,
    )
    assert [task.name for task in plan.tasks] == ['node-brz_a',
 'node-brz_b',
 'group-slv_a',
 'group-slv_b',
 'node-gld_a',
 'node-gld_b']
    assert [task.node_names for task in plan.tasks] == [['brz_a'],
 ['brz_b'],
 ['slv_a1', 'slv_a2'],
 ['slv_b2', 'slv_b1'],
 ['gld_a'],
 ['gld_b']]

    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_a", "slv_b"]
    )
    assert [task.name for task in plan.tasks] == [
 'group-slv_a',
 'group-slv_b',]
    assert [task.node_names for task in plan.tasks] == [
 ['slv_a1', 'slv_a2'],
 ['slv_b2', 'slv_b1'],
]

    plan = models.PipelineExecutionPlan(
        pipeline=pl,
        selects=["slv_a1*"]
    )
    assert [task.name for task in plan.tasks] == ['group-slv_a', 'node-gld_a']
    assert [task.node_names for task in plan.tasks] == [['slv_a1'], ['gld_a']]
