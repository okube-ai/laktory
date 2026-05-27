"""
Tests for SparkDeclarativePipelineOrchestrator (SDP).

Covers:
  - Build artifacts: spec file, config JSON, laktory_sdp.py
  - spec_dict content: name, schema, storage, libraries, configuration
  - config_dict: serialised pipeline
  - storage default (file:// + root_path)
  - CLI command construction (full_refresh, selects)
  - End-to-end execution (skipped when spark-pipelines is unavailable)
"""

import io
import shutil

import pyspark.sql.functions as F
import pytest
import yaml

from laktory import models
from laktory._settings import settings
from laktory._testing import StreamingSource

from ...conftest import assert_dfs_equal

_SDP_ORCH = {"type": "SPARK_DECLARATIVE_PIPELINE", "database": "default"}

# Inline pipeline with native PySpark transformers — used only by test_execute
# so that output DataFrames can be verified at the column level.
_EXEC_PIPELINE_YAML = """\
name: pl-sdp-exec
orchestrator:
  type: SPARK_DECLARATIVE_PIPELINE
  database: default
nodes:
  - name: brz
    source:
      format: JSON
      path: {tmp_path}/brz_source/
    sinks:
      - table_name: brz
        format: PARQUET
    transformer:
      dataframe_api: NATIVE
      nodes:
        - func_name: withColumn
          func_args:
            - zero
            - F.lit(0.0)
  - name: slv
    source:
      node_name: brz
    sinks:
      - table_name: slv
        format: PARQUET
    transformer:
      dataframe_api: NATIVE
      nodes:
        - func_name: withColumn
          func_args:
            - x2
            - F.col('x1')
"""


def is_sdp_available():
    return shutil.which("spark-pipelines") is not None


def _get_pl(tmp_path=""):
    return models.Pipeline.model_validate(
        {
            "name": "pl-declarative",
            "orchestrator": _SDP_ORCH,
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


def _get_pl_exec(tmp_path):
    data = _EXEC_PIPELINE_YAML.replace("{tmp_path}", str(tmp_path))
    return models.Pipeline.model_validate_yaml(io.StringIO(data))


# --------------------------------------------------------------------------- #
# Build artifacts                                                             #
# --------------------------------------------------------------------------- #


def test_spark_declarative_pipeline_build(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    pl.orchestrator.build()

    root_dir = pl.root_path
    assert (root_dir / "pl-declarative.json").exists()
    assert (root_dir / "laktory_sdp.py").exists()

    spec_path = pl.orchestrator.spec_filepath_abs
    assert spec_path == root_dir / "spark-pipeline.yaml"
    assert spec_path.exists()

    with open(spec_path) as fp:
        spec = yaml.safe_load(fp)

    assert spec["name"] == "pl-declarative"
    assert spec["schema"] == "default"
    assert spec["storage"] == "file://" + str(root_dir.absolute())
    assert spec["libraries"] == [{"glob": {"include": "laktory_sdp.py"}}]
    assert spec["configuration"]["laktory.is_sdp_execute"] == "true"
    assert "config_filepath" in spec["configuration"]


# --------------------------------------------------------------------------- #
# spec_dict and config_dict                                                   #
# --------------------------------------------------------------------------- #


def test_spec_dict(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    spec = pl.orchestrator.spec_dict

    assert spec["name"] == "pl-declarative"
    assert spec["schema"] == "default"
    assert spec["libraries"] == [{"glob": {"include": "laktory_sdp.py"}}]
    assert spec["configuration"]["laktory.is_sdp_execute"] == "true"
    assert "config_filepath" in spec["configuration"]


def test_storage_default(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    assert pl.orchestrator.storage == "file://" + str(pl.root_path.absolute())


# --------------------------------------------------------------------------- #
# is_orchestrator_sdp flags on nodes                                         #
# --------------------------------------------------------------------------- #


def test_is_orchestrator_sdp_on_nodes():
    pl = _get_pl()
    for node in pl.nodes:
        assert node.is_orchestrator_sdp is True
        assert node.is_orchestrator_ldp is False


# --------------------------------------------------------------------------- #
# Execute command construction                                                #
# --------------------------------------------------------------------------- #


def test_cli_flags(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    mock_run = mocker.patch("subprocess.run")

    # Default: no refresh flags
    pl.execute(use_orchestrator=True)
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "spark-pipelines"
    assert cmd[1] == "run"
    assert "--spec" in cmd
    assert cmd[cmd.index("--spec") + 1] == "spark-pipeline.yaml"
    assert mock_run.call_args[1]["cwd"] == pl.root_path.absolute()
    assert "--full-refresh-all" not in cmd
    assert "--full-refresh" not in cmd
    assert "--refresh" not in cmd

    # full_refresh only → --full-refresh-all
    mock_run.reset_mock()
    pl.execute(use_orchestrator=True, full_refresh=True)
    cmd = mock_run.call_args[0][0]
    assert "--full-refresh-all" in cmd

    # selects only → --refresh datasets
    mock_run.reset_mock()
    pl.execute(use_orchestrator=True, selects=["brz", "slv"])
    cmd = mock_run.call_args[0][0]
    assert "--refresh" in cmd
    assert cmd[cmd.index("--refresh") + 1] == "brz,slv"

    # full_refresh + selects → --full-refresh datasets
    mock_run.reset_mock()
    pl.execute(use_orchestrator=True, full_refresh=True, selects=["brz"])
    cmd = mock_run.call_args[0][0]
    assert "--full-refresh" in cmd
    assert cmd[cmd.index("--full-refresh") + 1] == "brz"


# --------------------------------------------------------------------------- #
# End-to-end execution                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute(tmp_path, monkeypatch, spark):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    ss = StreamingSource("PYSPARK")
    df0 = ss.write_to_json(tmp_path / "brz_source")
    df0 = df0.to_native()

    pl = _get_pl_exec(tmp_path)
    pl.orchestrator.execute(read_output=True)

    df_brz = df0.withColumn("zero", F.lit(0.0))
    df_slv = df_brz.withColumn("x2", F.col("x1"))

    assert_dfs_equal(pl.nodes[0].output_df, df_brz)
    assert_dfs_equal(pl.nodes[1].output_df, df_slv)


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute_declarative(tmp_path, monkeypatch, spark):
    """End-to-end SDP run covering batch view, streaming table, and pipeline view."""
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    ss = StreamingSource("PYSPARK")
    ss.write_to_json(tmp_path / "brz_source")

    pl = _get_pl(tmp_path)
    for node in pl.nodes:
        node.expectations = []
    pl.orchestrator.execute(read_output=True)

    nd = pl.nodes_dict
    assert nd["brz"].output_df is not None
    assert nd["slv"].output_df is not None
    assert nd["slv_stream"].output_df is not None
    assert nd["gld_view"].output_df is None  # temporary view — not persisted
