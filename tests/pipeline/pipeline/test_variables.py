"""Tests for variable injection in Pipeline fields."""

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
