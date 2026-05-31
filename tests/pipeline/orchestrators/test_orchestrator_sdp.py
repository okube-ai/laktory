"""
Tests for SparkDeclarativePipelineOrchestrator (SDP).

Covers:
  - Build artifacts: spec file, config JSON, laktory_sdp.py
  - spec_dict content: name, schema, storage, libraries, configuration
  - storage default (file:// + root_path)
  - CLI command construction (full_refresh, selects)
  - Decorator selection: @dp.materialized_view vs @dp.table vs @dp.temporary_view
  - Expectations: TypeError raised for non-SDP-compatible expectations
  - End-to-end execution (skipped when spark-pipelines is unavailable):
      NATIVE and NARWHALS transformers, SQL JOIN across nodes, value verification,
      batch + streaming + pipeline view, streaming incremental accumulation
"""

import io
import shutil

import narwhals as nw
import pyspark.sql.functions as F
import pytest
import yaml

from laktory import models
from laktory._settings import settings
from laktory._testing import StreamingSource

from ...conftest import assert_dfs_equal

_SDP_ORCH = {"type": "SPARK_DECLARATIVE_PIPELINE", "database": "default"}

# ---------------------------------------------------------------------------
# Pipeline YAML fixtures
# ---------------------------------------------------------------------------

# Four-node pipeline exercising NATIVE transformer, NARWHALS transformer,
# and a SQL JOIN across two upstream nodes.
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
  - name: slv_nw
    source:
      node_name: brz
    sinks:
      - table_name: slv_nw
        format: PARQUET
    transformer:
      dataframe_api: NARWHALS
      nodes:
        - func_name: with_columns
          func_kwargs:
            y1: x1
  - name: gld_join
    source:
      node_name: slv
    sinks:
      - table_name: gld_join
        format: PARQUET
    transformer:
      nodes:
        - expr: "SELECT {df}.id, {df}.x2, {nodes.slv_nw}.y1 FROM {df} LEFT JOIN\
 {nodes.slv_nw} ON {df}.id = {nodes.slv_nw}.id"
"""

# Single streaming node — used for the incremental accumulation test.
_STREAMING_PIPELINE_YAML = """\
name: pl-sdp-stream
orchestrator:
  type: SPARK_DECLARATIVE_PIPELINE
  database: default
nodes:
  - name: brz_stream
    source:
      format: DELTA
      path: {source_path}
      as_stream: true
    sinks:
      - table_name: brz_stream
"""


def is_sdp_available():
    return shutil.which("spark-pipelines") is not None


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------


def _get_pl(tmp_path=""):
    """Declarative pipeline: batch table, streaming table, pipeline view, expectations."""
    return models.Pipeline.model_validate(
        {
            "name": "pl-declarative",
            "orchestrator": _SDP_ORCH,
            "nodes": [
                {
                    "name": "brz",
                    "sources": {
                        "df": {"format": "JSON", "path": f"{tmp_path}/brz_source/"}
                    },
                    "sinks": [{"table_name": "brz"}],
                },
                {
                    "name": "slv",
                    "sources": {"df": {"node_name": "brz"}},
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
                    "sources": {"df": {"node_name": "brz", "as_stream": True}},
                    "sinks": [{"table_name": "slv_stream"}],
                },
                {
                    "name": "gld_view",
                    "sources": {"df": {"node_name": "slv"}},
                    "sinks": [{"pipeline_view_name": "gld_view"}],
                },
            ],
        }
    )


def _get_pl_exec(tmp_path):
    data = _EXEC_PIPELINE_YAML.replace("{tmp_path}", str(tmp_path))
    return models.Pipeline.model_validate_yaml(io.StringIO(data))


def _get_pl_stream(source_path):
    data = _STREAMING_PIPELINE_YAML.replace("{source_path}", str(source_path))
    return models.Pipeline.model_validate_yaml(io.StringIO(data))


# ---------------------------------------------------------------------------
# Build artifacts
# ---------------------------------------------------------------------------


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
    assert spec["configuration"]["laktory.executor"] == "SDP"
    assert "laktory.config_filepath" in spec["configuration"]


# ---------------------------------------------------------------------------
# spec_dict and storage
# ---------------------------------------------------------------------------


def test_spec_dict(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    spec = pl.orchestrator.spec_dict

    assert spec["name"] == "pl-declarative"
    assert spec["schema"] == "default"
    assert spec["libraries"] == [{"glob": {"include": "laktory_sdp.py"}}]
    assert spec["configuration"]["laktory.executor"] == "SDP"
    assert "laktory.config_filepath" in spec["configuration"]


def test_storage_default(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = _get_pl(tmp_path)
    assert pl.orchestrator.storage == "file://" + str(pl.root_path.absolute())


# ---------------------------------------------------------------------------
# is_sdp_execute: Spark conf flag detection
# ---------------------------------------------------------------------------


def test_is_sdp_execute_flag_true(spark):
    """is_sdp_execute() returns True when laktory.executor=SDP is in Spark conf."""
    import laktory

    _spark = laktory.get_spark_session()
    _spark.conf.set("laktory.executor", "SDP")
    try:
        assert laktory.is_sdp_execute() is True
    finally:
        _spark.conf.unset("laktory.executor")


def test_is_sdp_execute_flag_false_by_default(spark):
    """is_sdp_execute() returns False when laktory.executor is not set."""
    import laktory

    _spark = laktory.get_spark_session()
    try:
        _spark.conf.unset("laktory.executor")
    except Exception:
        pass
    assert laktory.is_sdp_execute() is False


# ---------------------------------------------------------------------------
# is_declarative_execute: covers both SDP and LDP
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "executor,expected",
    [
        ("SDP", True),
        ("LDP", True),
        ("AIRFLOW", False),
        ("", False),
    ],
)
def test_is_declarative_execute(spark, executor, expected):
    """is_declarative_execute() returns True for SDP and LDP, False otherwise."""
    import laktory

    _spark = laktory.get_spark_session()
    if executor:
        _spark.conf.set("laktory.executor", executor)
    else:
        try:
            _spark.conf.unset("laktory.executor")
        except Exception:
            pass
    try:
        assert laktory.is_declarative_execute() is expected
    finally:
        try:
            _spark.conf.unset("laktory.executor")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# is_orchestrator_sdp flags
# ---------------------------------------------------------------------------


def test_is_orchestrator_sdp_on_nodes():
    pl = _get_pl()
    for node in pl.nodes:
        assert node.is_orchestrator_sdp is True
        assert node.is_orchestrator_ldp is False


# ---------------------------------------------------------------------------
# Decorator selection: materialized_view / table / temporary_view
# ---------------------------------------------------------------------------


def test_sink_is_streaming_matches_decorator():
    """
    is_streaming() on a sink determines which SDP decorator laktory_sdp.py uses:
      False  → @dp.materialized_view  (batch static read)
      True   → @dp.table              (streaming read, as_stream=True)
      PipelineViewDataSink → @dp.temporary_view
    """
    from laktory.models.datasinks.pipelineviewdatasink import PipelineViewDataSink

    pl = _get_pl()

    # batch source → materialized_view
    assert pl.nodes_dict["slv"].sinks[0].is_streaming() is False

    # as_stream=True → table
    assert pl.nodes_dict["slv_stream"].sinks[0].is_streaming() is True

    # pipeline view sink → temporary_view
    assert isinstance(pl.nodes_dict["gld_view"].sinks[0], PipelineViewDataSink)


# ---------------------------------------------------------------------------
# Expectations: TypeError for non-SDP-compatible expectations
# ---------------------------------------------------------------------------


def test_expectations_raise_on_sdp_execute(monkeypatch):
    """WARN/DROP/FAIL expectations raise TypeError inside an SDP decorated function."""
    import laktory

    monkeypatch.setattr(laktory, "is_sdp_execute", lambda: True)

    pl = _get_pl()
    slv = pl.nodes_dict["slv"]
    slv._stage_df = True  # satisfies the non-None guard before the SDP check

    with pytest.raises(TypeError, match="not natively supported by SDP"):
        slv.check_expectations()


# ---------------------------------------------------------------------------
# CLI command construction
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# End-to-end execution
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute(tmp_path, monkeypatch, spark):
    """
    Four-node pipeline: NATIVE transformer, NARWHALS transformer, SQL JOIN node.
    Output values are verified end-to-end for each node.
    HiveMetastore default sink format is DELTA (overridden to PARQUET here for
    read-back simplicity, but the default is confirmed by test_sink_default_format).
    """
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    ss = StreamingSource("PYSPARK")
    src_df = ss.write_to_json(tmp_path / "brz_source").to_native()

    pl = _get_pl_exec(tmp_path)

    pl.nodes = pl.nodes[0:4]

    for node in pl.nodes:
        data = node.model_dump(exclude_unset=True)
        print(data)

    # raise ValueError()

    pl.orchestrator.execute(read_output=True)

    nd = pl.nodes_dict

    # brz: NATIVE PySpark transformer adds zero=0.0
    df_brz = src_df.withColumn("zero", F.lit(0.0))
    assert_dfs_equal(nd["brz"].output_df, df_brz)

    # slv: NATIVE PySpark transformer adds x2=x1
    df_slv = df_brz.withColumn("x2", F.col("x1"))
    assert_dfs_equal(nd["slv"].output_df, df_slv)

    # slv_nw: NARWHALS transformer adds y1=x1 (same value as x2 — different API)
    expected_slv_nw = nw.from_native(df_brz).with_columns(y1=nw.col("x1"))
    assert_dfs_equal(nd["slv_nw"].output_df, expected_slv_nw)

    # gld_join: SQL LEFT JOIN of slv (primary) and slv_nw on id
    # result columns: id, x2 (from slv), y1 (from slv_nw); x2 == y1 == x1
    expected_gld = (
        nw.from_native(df_slv).select("id", "x2").with_columns(y1=nw.col("x2"))
    )
    assert_dfs_equal(nd["gld_join"].output_df, expected_gld)


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_is_sdp_execute_true_during_execution(tmp_path, monkeypatch, spark):
    """
    Verifies that is_sdp_execute() returns True inside the spark-pipelines subprocess.

    laktory_sdp.py writes the return value of is_sdp_execute() to
    .laktory_is_sdp_execute in root_path immediately after reading the pipeline
    config.  Asserting that file contains "true" is a direct, robust check that
    is independent of is_sdp_compatible or any expectation logic.
    """
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    StreamingSource("PYSPARK").write_to_json(tmp_path / "brz_source")

    pl = _get_pl_exec(tmp_path)  # no expectations — pipeline completes successfully
    pl.orchestrator.execute()

    sentinel = pl.root_path / ".laktory_is_sdp_execute"
    assert sentinel.exists(), "laktory_sdp.py did not write the sentinel file"
    assert sentinel.read_text().strip() == "true"


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute_declarative(tmp_path, monkeypatch, spark):
    """
    Declarative pipeline: batch materialized_view, streaming table, temporary view.
    Expectations are cleared so that the run completes without error (the incompatibility
    is verified separately in test_expectations_raise_on_sdp_execute).
    Output values verified for batch nodes; streaming table row count verified.
    """
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    ss = StreamingSource("PYSPARK")
    src_df = ss.write_to_json(tmp_path / "brz_source")

    pl = _get_pl(tmp_path)
    for node in pl.nodes:
        node.expectations = []
    pl.orchestrator.execute(read_output=True)

    nd = pl.nodes_dict

    # brz and slv: pass-through nodes, output equals source data
    assert_dfs_equal(nd["brz"].output_df, src_df)
    assert_dfs_equal(nd["slv"].output_df, src_df)

    # slv_stream: streaming table accumulates 3 rows from one run
    assert nw.from_native(nd["slv_stream"].output_df).collect().shape[0] == 3

    # gld_view: temporary pipeline view — not persisted to warehouse
    assert nd["gld_view"].output_df is None


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
@pytest.mark.xfail(
    strict=False,
    reason=(
        "Requires Delta Lake in the spark-pipelines subprocess. "
        "Static Spark configs (spark.jars) cannot be forwarded via spec configuration "
        "because sql_conf re-applies them at runtime. "
        "Passes on Databricks where Delta is built-in."
    ),
)
def test_streaming_incremental(tmp_path, monkeypatch, spark):
    """
    Streaming SDP table accumulates new rows across successive pipeline runs.
    Run 1: source has 3 rows → streaming table has 3 rows.
    Run 2: 3 more rows appended to Delta source → streaming table has 6 rows.
    """
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    source_path = str(tmp_path / "delta_source")
    ss = StreamingSource("PYSPARK")
    ss.write_to_delta(source_path)  # batch 0: 3 rows written to Delta source

    pl = _get_pl_stream(source_path)
    pl.orchestrator.execute(read_output=True)
    assert nw.from_native(pl.nodes_dict["brz_stream"].output_df).collect().shape[0] == 3

    ss.write_to_delta(source_path)  # batch 1: 3 more rows appended → source has 6
    pl.orchestrator.execute(read_output=True)
    assert nw.from_native(pl.nodes_dict["brz_stream"].output_df).collect().shape[0] == 6
