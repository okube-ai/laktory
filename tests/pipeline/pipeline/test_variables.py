"""Tests for variable injection in Pipeline fields."""

import pytest
from pydantic import ValidationError

from laktory import models


def _pl_with_hive_sink(node_name="brz", pipeline_name="my-pipeline"):
    return models.Pipeline(
        name=pipeline_name,
        nodes=[
            models.PipelineNode(
                name=node_name,
                sinks=[{"schema_name": "default", "table_name": node_name}],
            )
        ],
    )


def test_pipeline_name_in_path():
    pl = _pl_with_hive_sink()
    pl.nodes[0].sinks[0]._setattr("table_name", "${{ pipeline.name }}")
    pl2 = pl.inject_vars()
    assert pl2.nodes[0].sinks[0].table_name == "my-pipeline"


def test_node_name_in_path():
    pl = _pl_with_hive_sink(node_name="brz")
    pl.nodes[0].sinks[0]._setattr("schema_name", "${{ pipeline_node.name }}")
    pl2 = pl.inject_vars()
    assert pl2.nodes[0].sinks[0].schema_name == "brz"


def test_node_source_explicit_type():
    """A pipeline node whose source explicitly sets `type` (a frozen field)
    must not crash inject_vars (#628)."""
    pl = models.Pipeline(
        name="my-pipeline",
        nodes=[
            models.PipelineNode(
                name="brz",
                sources=[
                    {
                        "type": "CUSTOM",
                        "custom_reader": "tests.datasources.test_customdatasource._read_polars",
                    }
                ],
                sinks=[{"schema_name": "default", "table_name": "brz"}],
            )
        ],
    )
    pl2 = pl.inject_vars()
    assert pl2.nodes[0].sources[0].type == "CUSTOM"


def test_pipeline_variables_dict():
    pl = models.Pipeline(
        name="my-pipeline",
        variables={"env": "prod"},
        nodes=[
            models.PipelineNode(
                name="brz",
                sinks=[{"schema_name": "default", "table_name": "brz"}],
            )
        ],
    )
    pl.nodes[0].sinks[0]._setattr("schema_name", "${{ vars.env }}")
    pl2 = pl.inject_vars()
    assert pl2.nodes[0].sinks[0].schema_name == "prod"


def _job_tasks(pl):
    return {
        t.task_key: [d.task_key for d in t.depends_on] for t in pl.orchestrator.task
    }


def test_placeholder_items():
    """A `${...}` item in a list of models (sources, sinks, transformer nodes)
    is validated into a model when injected."""
    pl = models.Pipeline(
        name="my-pipeline",
        variables={
            "source": {"node_name": "brz"},
            "sink": {"table_name": "slv"},
            "step": {"expr": "SELECT * FROM {df}"},
        },
        orchestrator={"type": "LAKEFLOW_JOB", "serverless_environment_version": "3"},
        nodes=[
            {
                "name": "brz",
                "sources": [{"table_name": "src"}],
                "sinks": [{"table_name": "brz"}],
            },
            {
                "name": "slv",
                "sources": ["${vars.source}"],
                "transformer": {"nodes": ["${vars.step}"]},
                "sinks": ["${vars.sink}"],
            },
        ],
    )
    node = pl.nodes[1]
    assert node.sources == ["${vars.source}"]
    assert node.data_sources == []
    assert _job_tasks(pl) == {"node-brz": [], "node-slv": []}

    pl2 = pl.inject_vars()
    node = pl2.nodes[1]
    assert type(node.sources[0]).__name__ == "PipelineNodeDataSource"
    assert type(node.sinks[0]).__name__ == "UnityCatalogDataSink"
    assert type(node.transformer.nodes[0]).__name__ == "DataFrameExpr"
    assert node.sources[0].parent is node
    assert node.sinks[0].parent is node
    assert node.upstream_node_names == ["brz"]

    # Orchestrator tasks are rebuilt from the injected DAG
    assert _job_tasks(pl2) == {"node-brz": [], "node-slv": ["node-brz"]}

    # Source model untouched
    assert pl.nodes[1].sources == ["${vars.source}"]
    assert _job_tasks(pl) == {"node-brz": [], "node-slv": []}


def test_placeholder_sources():
    """A `${...}` for the whole `sources` list does not break the node before injection."""
    pl = models.Pipeline(
        name="my-pipeline",
        variables={"sources": [{"node_name": "brz"}]},
        nodes=[
            {
                "name": "brz",
                "sources": [{"table_name": "src"}],
                "sinks": [{"table_name": "brz"}],
            },
            {
                "name": "slv",
                "sources": "${vars.sources}",
                "sinks": [{"table_name": "slv"}],
            },
        ],
    )
    assert pl.nodes[1].data_sources == []
    pl2 = pl.inject_vars()
    assert pl2.nodes[1].upstream_node_names == ["brz"]
    assert pl2.nodes[1].sources[0].parent is pl2.nodes[1]


def test_inject_vars_rebuilds(monkeypatch):
    """Orchestrator tasks are rebuilt only when injected variables change the
    DAG, not for other variables (e.g. in a list of strings) or on a cache hit."""
    from laktory.models.pipeline.orchestrators.lakeflowjoborchestrator import (
        LakeflowJobOrchestrator,
    )

    rebuilds = []
    update_from_parent = LakeflowJobOrchestrator.update_from_parent

    def _update_from_parent(self):
        rebuilds.append(1)
        update_from_parent(self)

    def _pl(source):
        return models.Pipeline(
            name="my-pipeline",
            variables={"version": "1.0", "source": {"node_name": "brz"}},
            dependencies=["laktory==${vars.version}"],
            orchestrator={
                "type": "LAKEFLOW_JOB",
                "serverless_environment_version": "3",
            },
            nodes=[
                {
                    "name": "brz",
                    "sources": [{"table_name": "src"}],
                    "sinks": [{"table_name": "brz"}],
                },
                {"name": "slv", "sources": [source], "sinks": [{"table_name": "slv"}]},
            ],
        )

    for source, expected in [({"node_name": "brz"}, 0), ("${vars.source}", 1)]:
        pl = _pl(source)
        monkeypatch.setattr(
            LakeflowJobOrchestrator, "update_from_parent", _update_from_parent
        )
        rebuilds.clear()
        pl2 = pl.inject_vars()
        assert len(rebuilds) == expected
        assert pl2.dependencies == ["laktory==1.0"]
        assert _job_tasks(pl2) == {"node-brz": [], "node-slv": ["node-brz"]}

        # Cache hit
        rebuilds.clear()
        pl3 = pl.inject_vars()
        assert rebuilds == []
        assert _job_tasks(pl3) == {"node-brz": [], "node-slv": ["node-brz"]}
        assert pl3.orchestrator.parent is pl3
        monkeypatch.undo()


@pytest.mark.parametrize(
    "orchestrator",
    [
        {"type": "SPARK_DECLARATIVE_PIPELINE"},
        {"type": "AIRFLOW"},
        "${vars.orchestrator}",
    ],
)
def test_placeholder_source_orchestrators(orchestrator):
    pl = models.Pipeline(
        name="my-pipeline",
        variables={
            "source": {"node_name": "brz"},
            "orchestrator": {"type": "SPARK_DECLARATIVE_PIPELINE"},
        },
        orchestrator=orchestrator,
        nodes=[
            {
                "name": "brz",
                "sources": [{"table_name": "src"}],
                "sinks": [{"table_name": "brz"}],
            },
            {
                "name": "slv",
                "sources": ["${vars.source}"],
                "sinks": [{"table_name": "slv"}],
            },
        ],
    )
    pl2 = pl.inject_vars()
    assert pl2.nodes[1].upstream_node_names == ["brz"]
    assert pl2.orchestrator.parent is pl2


@pytest.mark.parametrize(
    "orchestrator", ["LAKEFLOW_DECLARATIVE_PIPELINE", "SPARK_DECLARATIVE_PIPELINE"]
)
def test_placeholder_sink_declarative(orchestrator):
    """A sink resolved from a variable gets the orchestrator catalog and
    schema, and is validated against the orchestrator once injected."""

    def _pl(sink):
        return models.Pipeline(
            name="my-pipeline",
            variables={"sink": sink},
            orchestrator={"type": orchestrator, "catalog": "c", "schema": "s"},
            nodes=[
                {
                    "name": "brz",
                    "sources": [{"table_name": "src"}],
                    "sinks": [{"table_name": "a"}, "${vars.sink}"],
                }
            ],
        )

    pl = _pl({"table_name": "b"})
    assert [s.full_name for s in pl.inject_vars().nodes[0].sinks] == ["c.s.a", "c.s.b"]

    if orchestrator == "LAKEFLOW_DECLARATIVE_PIPELINE":
        pl = _pl({"path": "/tmp/data", "format": "PARQUET"})
        with pytest.raises(ValidationError, match="not supported with Lakeflow"):
            pl.inject_vars()

        # Index of the item in the sinks as declared, placeholders included
        with pytest.raises(ValidationError, match=r"sinks\[1\]"):
            models.Pipeline(
                name="my-pipeline",
                orchestrator={"type": orchestrator, "catalog": "c", "schema": "s"},
                nodes=[
                    {
                        "name": "brz",
                        "sources": [{"table_name": "src"}],
                        "sinks": [
                            "${vars.sink}",
                            {"path": "/tmp/x", "format": "PARQUET"},
                        ],
                    }
                ],
            )


def test_placeholder_transformer():
    """A whole transformer from a variable is accepted, then validated into a
    model and wired into the DAG and the job tasks once injected."""
    pl = models.Pipeline(
        name="my-pipeline",
        variables={"t": {"nodes": [{"expr": "SELECT * FROM {nodes.brz}"}]}},
        orchestrator={"type": "LAKEFLOW_JOB", "serverless_environment_version": "3"},
        nodes=[
            {
                "name": "brz",
                "sources": [{"table_name": "src"}],
                "sinks": [{"table_name": "brz"}],
            },
            {
                "name": "slv",
                "sources": [{"table_name": "src"}],
                "transformer": "${vars.t}",
                "sinks": [{"table_name": "slv"}],
            },
        ],
    )
    assert pl.nodes[1].upstream_node_names == []

    pl2 = pl.inject_vars()
    node = pl2.nodes[1]
    assert isinstance(node.transformer, models.DataFrameTransformer)
    assert node.transformer.parent_pipeline_node is node
    assert node.upstream_node_names == ["brz"]
    depends_on = {
        t.task_key: [d.task_key for d in t.depends_on or []]
        for t in pl2.orchestrator.task
    }
    assert depends_on == {"node-brz": [], "node-slv": ["node-brz"]}


def test_placeholder_transformer_view():
    """A VIEW sink with a transformer from a variable is validated once injected."""
    node = {
        "name": "slv",
        "sources": [{"table_name": "src"}],
        "transformer": "${vars.t}",
        "sinks": [{"table_name": "slv", "table_type": "VIEW"}],
    }
    pl = models.Pipeline(
        name="my-pipeline",
        variables={"t": {"nodes": [{"expr": "SELECT * FROM {sources.src}"}]}},
        nodes=[node],
    )
    pl2 = pl.inject_vars()
    assert pl2.nodes[0].is_view
    assert pl2.nodes[0].view_definition is not None

    pl = models.Pipeline(
        name="my-pipeline",
        variables={"t": {"nodes": [{"func_name": "drop_duplicates"}]}},
        nodes=[node],
    )
    with pytest.raises(ValidationError, match="VIEW sink requires a transformer"):
        pl.inject_vars()
