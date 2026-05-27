"""
Tests for LakeflowDeclarativePipelineOrchestrator (LDP) and comparative
behaviour against SparkDeclarativePipelineOrchestrator (SDP).

Feature matrix:
  - Orchestrator type flags (is_orchestrator_ldp / is_orchestrator_sdp)
  - Schema propagation from orchestrator to table sinks
  - sdp_table_or_view_kwargs per dataset type (batch, streaming, pipeline view)
  - is_streaming() on table sinks derived from source as_stream flag
  - LDP expectations: WARN/DROP/FAIL produce non-empty dicts on node and sink
  - LDP streaming context: incompatible expectation type raises TypeError
  - LDP configuration: requirements (JSON list including laktory), config_filepath
  - LDP guard: pipeline view nodes raise ValueError
"""

import json

import pytest
from pydantic import ValidationError

from laktory import models
from laktory.models.datasinks.pipelineviewdatasink import PipelineViewDataSink

_LDP_ORCH = {
    "type": "LAKEFLOW_DECLARATIVE_PIPELINE",
    "catalog": "dev",
    "schema": "sandbox",
}
_SDP_ORCH = {
    "type": "SPARK_DECLARATIVE_PIPELINE",
    "database": "default",
}

_ORCHESTRATORS = [
    pytest.param(_LDP_ORCH, id="ldp"),
    pytest.param(_SDP_ORCH, id="sdp"),
]


def _get_pl(orchestrator_dict, tmp_path=""):
    return models.Pipeline.model_validate(
        {
            "name": "pl-declarative",
            "orchestrator": orchestrator_dict,
            "nodes": [
                {
                    "name": "brz",
                    "source": {"format": "JSON", "path": f"{tmp_path}/brz_source/"},
                    "sinks": [{"table_name": "brz"}],
                },
                {
                    "name": "slv",
                    "source": {"node_name": "brz"},
                    "sinks": [{"table_name": "slv"}],
                    "expectations": [
                        {"name": "x1 positive", "expr": "x1 > 0", "action": "WARN"},
                        {
                            "name": "id not null",
                            "expr": "id IS NOT NULL",
                            "action": "DROP",
                        },
                        {
                            "name": "x1 not negative",
                            "expr": "x1 >= 0",
                            "action": "FAIL",
                        },
                    ],
                },
                {
                    "name": "slv_stream",
                    "source": {"node_name": "brz", "as_stream": True},
                    "sinks": [{"table_name": "slv_stream"}],
                },
                {
                    "name": "gld_view",
                    "source": {"node_name": "slv"},
                    "sinks": [{"pipeline_view_name": "gld_view"}],
                },
            ],
        }
    )


# --------------------------------------------------------------------------- #
# Orchestrator type flags                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("orchestrator_dict", _ORCHESTRATORS)
def test_orchestrator_type(orchestrator_dict):
    pl = _get_pl(orchestrator_dict)
    is_ldp = orchestrator_dict["type"] == "LAKEFLOW_DECLARATIVE_PIPELINE"
    assert pl.is_orchestrator_ldp == is_ldp
    assert pl.is_orchestrator_sdp == (not is_ldp)


# --------------------------------------------------------------------------- #
# Schema propagation                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("orchestrator_dict", _ORCHESTRATORS)
def test_schema_propagation(orchestrator_dict):
    pl = _get_pl(orchestrator_dict)
    is_ldp = orchestrator_dict["type"] == "LAKEFLOW_DECLARATIVE_PIPELINE"

    for node in pl.nodes:
        for sink in node.all_sinks:
            if isinstance(sink, PipelineViewDataSink):
                continue
            if is_ldp:
                assert sink.catalog_name == "dev", f"node={node.name}"
                assert sink.schema_name == "sandbox", f"node={node.name}"
            else:
                assert sink.catalog_name is None, f"node={node.name}"
                assert sink.schema_name == "default", f"node={node.name}"


# --------------------------------------------------------------------------- #
# sdp_table_or_view_kwargs                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("orchestrator_dict", _ORCHESTRATORS)
def test_sdp_table_or_view_kwargs(orchestrator_dict):
    pl = _get_pl(orchestrator_dict)
    is_ldp = orchestrator_dict["type"] == "LAKEFLOW_DECLARATIVE_PIPELINE"
    nd = pl.nodes_dict

    # Batch table: LDP uses full UC name; SDP uses table name only (no catalog)
    brz_kwargs = nd["brz"].sinks[0].sdp_table_or_view_kwargs
    if is_ldp:
        assert brz_kwargs == {"name": "dev.sandbox.brz"}
    else:
        assert brz_kwargs == {"name": "brz"}

    # Streaming table: same name logic as batch
    stream_kwargs = nd["slv_stream"].sinks[0].sdp_table_or_view_kwargs
    if is_ldp:
        assert stream_kwargs == {"name": "dev.sandbox.slv_stream"}
    else:
        assert stream_kwargs == {"name": "slv_stream"}

    # Pipeline view: always keyed by view name regardless of orchestrator
    view_sink = nd["gld_view"].sinks[0]
    assert isinstance(view_sink, PipelineViewDataSink)
    assert view_sink.sdp_table_or_view_kwargs == {"name": "gld_view"}


# --------------------------------------------------------------------------- #
# is_streaming                                                                #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("orchestrator_dict", _ORCHESTRATORS)
def test_is_streaming(orchestrator_dict):
    pl = _get_pl(orchestrator_dict)
    nd = pl.nodes_dict

    assert nd["brz"].sinks[0].is_streaming() is False
    assert nd["slv"].sinks[0].is_streaming() is False
    assert nd["slv_stream"].sinks[0].is_streaming() is True
    assert nd["gld_view"].sinks[0].is_streaming() is False


# --------------------------------------------------------------------------- #
# Expectations — LDP                                                          #
# --------------------------------------------------------------------------- #


def test_ldp_expectations():
    pl = _get_pl(_LDP_ORCH)
    slv = pl.nodes_dict["slv"]

    assert slv.ldp_warning_expectations == {"x1 positive": "x1 > 0"}
    assert slv.ldp_drop_expectations == {"id not null": "id IS NOT NULL"}
    assert slv.ldp_fail_expectations == {"x1 not negative": "x1 >= 0"}

    slv_sink = slv.sinks[0]
    assert slv_sink.ldp_warning_expectations == {"x1 positive": "x1 > 0"}
    assert slv_sink.ldp_drop_expectations == {"id not null": "id IS NOT NULL"}
    assert slv_sink.ldp_fail_expectations == {"x1 not negative": "x1 >= 0"}

    brz = pl.nodes_dict["brz"]
    assert brz.ldp_warning_expectations == {}
    assert brz.ldp_drop_expectations == {}
    assert brz.ldp_fail_expectations == {}


# --------------------------------------------------------------------------- #
# Expectations — SDP                                                          #
# --------------------------------------------------------------------------- #


def test_sdp_expectations_empty():
    pl = _get_pl(_SDP_ORCH)
    slv = pl.nodes_dict["slv"]

    assert slv.sdp_warning_expectations == {}
    assert slv.sdp_drop_expectations == {}
    assert slv.sdp_fail_expectations == {}

    slv_sink = slv.sinks[0]
    assert slv_sink.sdp_warning_expectations == {}
    assert slv_sink.sdp_drop_expectations == {}
    assert slv_sink.sdp_fail_expectations == {}


def test_sdp_execute_raises_for_expectations(monkeypatch):
    import laktory

    monkeypatch.setattr(laktory, "is_sdp_execute", lambda: True)

    pl = _get_pl(_SDP_ORCH)
    slv = pl.nodes_dict["slv"]
    slv._stage_df = True  # non-None; SDP guard runs before any df method calls

    with pytest.raises(TypeError, match="not natively supported by SDP"):
        slv.check_expectations()


# --------------------------------------------------------------------------- #
# Expectations — LDP streaming                                                #
# --------------------------------------------------------------------------- #


def test_ldp_streaming_incompatible_expectations_raise(monkeypatch, spark):
    import narwhals as nw

    import laktory
    from laktory.models import DataQualityExpectation

    monkeypatch.setattr(laktory, "is_ldp_execute", lambda: True)

    pl = _get_pl(_LDP_ORCH)
    slv = pl.nodes_dict["slv"]

    slv.expectations = [
        DataQualityExpectation(
            name="row count ok", expr="COUNT(*) > 0", type="AGGREGATE"
        )
    ]

    streaming_df = spark.readStream.format("rate").load().limit(5)
    slv._stage_df = nw.from_native(streaming_df)

    with pytest.raises(TypeError, match="not natively supported by Lakeflow"):
        slv.check_expectations()


# --------------------------------------------------------------------------- #
# LDP configuration: requirements and config_filepath                         #
# --------------------------------------------------------------------------- #


def test_ldp_configuration_requirements():
    """configuration['requirements'] is a JSON list that includes laktory."""
    pl = _get_pl(_LDP_ORCH)
    conf = pl.orchestrator.configuration

    assert "requirements" in conf
    reqs = json.loads(conf["requirements"])
    assert isinstance(reqs, list)
    assert any("laktory" in r for r in reqs)


def test_ldp_configuration_requirements_custom_dep():
    """Extra pipeline dependencies appear in configuration['requirements']."""
    pl = models.Pipeline.model_validate(
        {
            "name": "pl-declarative",
            "orchestrator": _LDP_ORCH,
            "dependencies": ["my-package==1.2.3"],
            "nodes": [
                {
                    "name": "brz",
                    "source": {"format": "JSON", "path": "/src/"},
                    "sinks": [{"table_name": "brz"}],
                }
            ],
        }
    )
    reqs = json.loads(pl.orchestrator.configuration["requirements"])
    assert any("my-package" in r for r in reqs)
    assert any("laktory" in r for r in reqs)


def test_ldp_configuration_filepath():
    """configuration['config_filepath'] starts with /Workspace and encodes the pipeline name."""
    pl = _get_pl(_LDP_ORCH)
    conf = pl.orchestrator.configuration

    assert "config_filepath" in conf
    fp = conf["config_filepath"]
    assert fp.startswith("/Workspace")
    assert pl.name in fp
    assert fp.endswith(".json")


# --------------------------------------------------------------------------- #
# LDP guard: view nodes are rejected                                          #
# --------------------------------------------------------------------------- #


def test_ldp_view_node_raises():
    """A TableDataSink with table_type=VIEW (SQL view) must raise with LAKEFLOW_DECLARATIVE_PIPELINE."""
    with pytest.raises((ValueError, ValidationError), match="view"):
        models.Pipeline.model_validate(
            {
                "name": "pl-declarative",
                "orchestrator": _LDP_ORCH,
                "nodes": [
                    {
                        "name": "brz",
                        "source": {"format": "JSON", "path": "/src/"},
                        "sinks": [{"table_name": "brz"}],
                    },
                    {
                        "name": "gld_view",
                        "source": {"node_name": "brz"},
                        "sinks": [{"table_name": "gld_view", "table_type": "VIEW"}],
                    },
                ],
            }
        )
