"""Tests for the discriminated unions of sources, sinks, transformer nodes and orchestrators."""

import pytest
from pydantic import ValidationError

from laktory import models


def _errors(cls, data):
    with pytest.raises(ValidationError) as excinfo:
        cls.model_validate(data)
    return excinfo.value.errors()


@pytest.mark.parametrize(
    "sink,cls",
    [
        ({"table_name": "t"}, "UnityCatalogDataSink"),
        ({"schema_name": "s", "table_name": "t"}, "UnityCatalogDataSink"),
        ({"path": "/data", "format": "PARQUET"}, "FileDataSink"),
        ({"pipeline_view_name": "v"}, "PipelineViewDataSink"),
        ({"table_name": "t", "type": "HIVE_METASTORE"}, "HiveMetastoreDataSink"),
        ({"path": "/data", "format": "PARQUET", "type": "FILE"}, "FileDataSink"),
    ],
)
def test_sink_selection(sink, cls):
    node = models.PipelineNode(name="n", sinks=[sink])
    assert type(node.sinks[0]).__name__ == cls


@pytest.mark.parametrize(
    "sink",
    [
        {"path": "/data", "format": "PARQUET", "type": "${vars.t}"},
        {"table_name": "t", "type": "${vars.t}"},
    ],
)
def test_sink_type_variable(sink):
    # The class is selected when parsing: a variable `type` resolved later
    # could contradict it
    (error,) = _errors(models.PipelineNode, {"name": "n", "sinks": [sink]})
    assert error["type"] == "invalid_sink_type"
    assert error["loc"] == ("sinks", 0)


def test_type_selector_not_variable():
    for cls in [
        models.UnityCatalogDataSink,
        models.HiveMetastoreDataSink,
        models.FileDataSink,
        models.PipelineViewDataSink,
        models.UnityCatalogDataSource,
        models.FileDataSource,
        models.PipelineNodeDataSource,
    ]:
        assert "VariableType" not in str(cls.model_fields["type"].annotation), cls


@pytest.mark.parametrize(
    "source,cls",
    [
        ({"table_name": "t"}, "UnityCatalogDataSource"),
        ({"path": "/data", "format": "JSON"}, "FileDataSource"),
        ({"node_name": "brz"}, "PipelineNodeDataSource"),
        ({"custom_reader": "my_reader"}, "CustomDataSource"),
        ({"data": {"x": [1, 2]}}, "DataFrameDataSource"),
        ({"table_name": "t", "type": "HIVE_METASTORE"}, "HiveMetastoreDataSource"),
        ({"node_name": "brz", "type": "PIPELINE_NODE"}, "PipelineNodeDataSource"),
    ],
)
def test_source_selection(source, cls):
    node = models.PipelineNode(name="n", sources=[source])
    assert type(node.sources[0]).__name__ == cls


def test_transformer_node_selection():
    t = models.DataFrameTransformer(
        nodes=[
            {"func_name": "drop", "func_args": ["x"]},
            {"expr": "SELECT * FROM {df}"},
            {"expr": "SELECT * FROM {df}", "type": "SQL"},
        ]
    )
    assert [type(n).__name__ for n in t.nodes] == [
        "DataFrameMethod",
        "DataFrameExpr",
        "DataFrameExpr",
    ]


def test_instances_and_placeholders():
    sink = models.FileDataSink(path="/data", format="PARQUET")
    source = models.UnityCatalogDataSource(table_name="t")
    node = models.PipelineNode(name="n", sources=[source], sinks=[sink])
    assert node.sinks[0] is sink
    assert node.sources[0] is source

    # Whole list as a variable
    node = models.PipelineNode(
        name="n",
        sinks="${vars.sinks}",
        variables={"sinks": [{"path": "/data", "format": "PARQUET"}]},
    )
    assert node.sinks == "${vars.sinks}"
    node = node.inject_vars()
    assert type(node.sinks[0]).__name__ == "FileDataSink"


def test_single_error():
    # Invalid field: one error, reported against the selected member only
    errors = _errors(
        models.PipelineNode, {"name": "n", "sinks": [{"table_name": "t", "modee": 1}]}
    )
    assert len(errors) == 1
    assert errors[0]["loc"] == ("sinks", 0, "UnityCatalogDataSink", "modee")

    errors = _errors(
        models.PipelineNode,
        {
            "name": "n",
            "sinks": [{"table_name": "t", "mode": "OVERWRITE", "shared": True}],
        },
    )
    assert len(errors) == 1
    assert "only support `APPEND` mode" in errors[0]["msg"]

    errors = _errors(
        models.PipelineNode, {"name": "n", "sources": [{"node_name": "a", "foo": 1}]}
    )
    assert len(errors) == 1
    assert errors[0]["loc"] == ("sources", 0, "PipelineNodeDataSource", "foo")

    errors = _errors(
        models.DataFrameTransformer, {"nodes": [{"expr": "SELECT 1", "foo": 1}]}
    )
    assert len(errors) == 1
    assert errors[0]["loc"] == ("nodes", 0, "DataFrameExpr", "foo")

    # String that is not a placeholder
    errors = _errors(models.PipelineNode, {"name": "n", "sinks": ["vars.sink"]})
    assert len(errors) == 1
    assert errors[0]["loc"] == ("sinks", 0, "VariableType")


def test_invalid_type():
    errors = _errors(
        models.PipelineNode,
        {"name": "n", "sinks": [{"table_name": "t", "type": "UNITY_CATLOG"}]},
    )
    assert len(errors) == 1
    assert errors[0]["type"] == "invalid_sink_type"
    assert "'UNITY_CATALOG'" in errors[0]["msg"]

    errors = _errors(
        models.PipelineNode, {"name": "n", "sources": [{"node_name": "a", "type": "X"}]}
    )
    assert len(errors) == 1
    assert errors[0]["type"] == "invalid_source_type"


def test_orchestrator():
    pl = models.Pipeline(
        name="pl", orchestrator={"type": "SPARK_DECLARATIVE_PIPELINE", "catalog": "dev"}
    )
    assert type(pl.orchestrator).__name__ == "SparkDeclarativePipelineOrchestrator"

    errors = _errors(
        models.Pipeline,
        {"name": "pl", "orchestrator": {"type": "LAKEFLOW_JOB", "x": 1}},
    )
    assert len(errors) == 1
    assert errors[0]["loc"] == ("orchestrator", "LakeflowJobOrchestrator", "x")

    # `type` is required
    errors = _errors(
        models.Pipeline, {"name": "pl", "orchestrator": {"catalog": "dev"}}
    )
    assert len(errors) == 1
    assert errors[0]["type"] == "invalid_orchestrator_type"


def test_single_error_in_stack():
    node = {"name": "n", "sinks": [{"table_name": "t", "modee": 1}]}
    errors = _errors(
        models.Stack,
        {
            "name": "s",
            "resources": {"pipelines": {"p": {"name": "p", "nodes": [node]}}},
        },
    )
    assert len(errors) == 1
    assert errors[0]["loc"] == (
        "resources",
        "pipelines",
        "p",
        "nodes",
        0,
        "sinks",
        0,
        "UnityCatalogDataSink",
        "modee",
    )
