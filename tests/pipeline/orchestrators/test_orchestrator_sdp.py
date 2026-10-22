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
from types import SimpleNamespace

import narwhals as nw
import pyspark.sql.functions as F
import pytest
import yaml

from laktory import models
from laktory._settings import settings
from laktory._testing import StreamingSource

from ...conftest import SDP_TEST_CONFIGURATION
from ...conftest import assert_dfs_equal

_SDP_ORCH = {
    "type": "SPARK_DECLARATIVE_PIPELINE",
    "database": "default",
    "configuration": SDP_TEST_CONFIGURATION,
}

# ---------------------------------------------------------------------------
# Pipeline YAML fixtures
# ---------------------------------------------------------------------------

# Single streaming node - used for the incremental accumulation test.
_STREAMING_PIPELINE_YAML = """\
name: pl-sdp-stream
orchestrator:
  type: SPARK_DECLARATIVE_PIPELINE
  database: default
nodes:
  - name: brz_stream
    sources:
    - format: DELTA
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
                    "sources": [{"format": "JSON", "path": f"{tmp_path}/brz_source/"}],
                    "sinks": [{"table_name": "brz"}],
                },
                {
                    "name": "slv",
                    "sources": [{"node_name": "brz"}],
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
                    "sources": [{"node_name": "brz", "as_stream": True}],
                    "sinks": [{"table_name": "slv_stream"}],
                },
                {
                    "name": "gld_view",
                    "sources": [{"node_name": "slv"}],
                    "sinks": [{"pipeline_view_name": "gld_view"}],
                },
            ],
        }
    )


def _get_pl_stream(source_path):
    data = _STREAMING_PIPELINE_YAML.replace("{source_path}", str(source_path))
    pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
    pl.orchestrator.configuration = SDP_TEST_CONFIGURATION
    return pl


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


def test_launch_args(monkeypatch, tmp_path):
    from pyspark.errors import PySparkNotImplementedError

    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    orchestrator = _get_pl(tmp_path).orchestrator

    # Classic session: Delta configuration forwarded
    conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }
    spark = SimpleNamespace(sparkContext=SimpleNamespace(getConf=lambda: conf))
    args = orchestrator._get_launch_args(spark)
    assert "spark.sql.sources.default=delta" in args
    assert "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" in args

    # Spark Connect session: no `sparkContext`, nothing forwarded
    class _ConnectSession:
        @property
        def sparkContext(self):
            # Message-only form: kwargs were renamed between PySpark 3
            # (`error_class`) and 4 (`errorClass`)
            raise PySparkNotImplementedError("sparkContext() is not implemented")

    assert orchestrator._get_launch_args(_ConnectSession()) == []


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
    assert "run" in cmd
    assert "--spec" in cmd
    assert cmd[cmd.index("--spec") + 1] == "spark-pipeline.yaml"
    assert mock_run.call_args[1]["cwd"] == pl.root_path.absolute()
    assert "--full-refresh-all" not in cmd
    assert "--full-refresh" not in cmd
    assert "--refresh" not in cmd

    # Local Spark session jars and Delta configuration forwarded before `run`
    launch_args = cmd[1 : cmd.index("run")]
    assert "--driver-class-path" in launch_args
    jars = launch_args[launch_args.index("--driver-class-path") + 1]
    assert "delta-spark" in jars
    assert "file:" not in jars
    assert "jackson-databind" not in jars  # bundled with Spark
    assert "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" in launch_args
    assert "spark.sql.sources.default=delta" in launch_args

    # full_refresh only → --full-refresh-all
    mock_run.reset_mock()
    pl.execute(use_orchestrator=True, refresh="full")
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
    pl.execute(use_orchestrator=True, refresh="full", selects=["brz"])
    cmd = mock_run.call_args[0][0]
    assert "--full-refresh" in cmd
    assert cmd[cmd.index("--full-refresh") + 1] == "brz"


# ---------------------------------------------------------------------------
# End-to-end execution
# ---------------------------------------------------------------------------
# Each `spark-pipelines` run starts a new JVM and Spark session (~15s), so the
# end-to-end scenarios share a single pipeline run: the module-scoped `e2e`
# fixture executes one pipeline combining all of them, and each test asserts on
# its own nodes. See `.claude/docs/testing.md` (SDP end-to-end tests).

_skip_no_sdp = pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)


def _get_pl_e2e(tmp_path):
    """
    Combined end-to-end pipeline. Scenarios:
      - transformers: NATIVE (brz, slv), NARWHALS (slv_nw) and SQL JOIN (gld_join,
        PARQUET sink) nodes
      - declarative: materialized view (dec_slv), streaming table (dec_slv_stream)
        and temporary view (dec_gld_view)
      - shared sink: two streaming nodes (feed_a, feed_b) appending to one table
    """
    return models.Pipeline.model_validate(
        {
            "name": "pl-sdp-e2e",
            "root_path": str(tmp_path / "pl-sdp-e2e"),
            "orchestrator": _SDP_ORCH,
            "nodes": [
                # Transformers
                {
                    "name": "brz",
                    "sources": [{"format": "JSON", "path": f"{tmp_path}/brz_source/"}],
                    "sinks": [{"table_name": "brz"}],
                    "transformer": {
                        "dataframe_api": "NATIVE",
                        "nodes": [
                            {
                                "func_name": "withColumn",
                                "func_args": ["zero", "F.lit(0.0)"],
                            }
                        ],
                    },
                },
                {
                    "name": "slv",
                    "sources": [{"node_name": "brz"}],
                    "sinks": [{"table_name": "slv"}],
                    "transformer": {
                        "dataframe_api": "NATIVE",
                        "nodes": [
                            {
                                "func_name": "withColumn",
                                "func_args": ["x2", "F.col('x1')"],
                            }
                        ],
                    },
                },
                {
                    "name": "slv_nw",
                    "sources": [{"node_name": "brz"}],
                    "sinks": [{"table_name": "slv_nw"}],
                    "transformer": {
                        "dataframe_api": "NARWHALS",
                        "nodes": [
                            {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}}
                        ],
                    },
                },
                {
                    "name": "gld_join",
                    "sources": [{"node_name": "slv"}],
                    "sinks": [{"table_name": "gld_join", "format": "PARQUET"}],
                    "transformer": {
                        "nodes": [
                            {
                                "expr": "SELECT {df}.id, {df}.x2, {nodes.slv_nw}.y1 "
                                "FROM {df} LEFT JOIN {nodes.slv_nw} "
                                "ON {df}.id = {nodes.slv_nw}.id"
                            }
                        ]
                    },
                },
                # Declarative datasets
                {
                    "name": "dec_slv",
                    "sources": [{"node_name": "brz"}],
                    "sinks": [{"table_name": "dec_slv"}],
                },
                {
                    "name": "dec_slv_stream",
                    "sources": [{"node_name": "brz", "as_stream": True}],
                    "sinks": [{"table_name": "dec_slv_stream"}],
                },
                {
                    "name": "dec_gld_view",
                    "sources": [{"node_name": "dec_slv"}],
                    "sinks": [{"pipeline_view_name": "dec_gld_view"}],
                },
                # Shared sink
                *[
                    {
                        "name": name,
                        "sources": [{"node_name": "brz", "as_stream": True}],
                        "sinks": [{"table_name": "shared"}],
                    }
                    for name in ["feed_a", "feed_b"]
                ],
            ],
        }
    )


@pytest.fixture(scope="module")
def e2e(tmp_path_factory):
    """Single `spark-pipelines` run of the combined end-to-end pipeline"""
    if not is_sdp_available():
        pytest.skip("spark-pipelines CLI not available (requires PySpark 4.1+)")

    tmp_path = tmp_path_factory.mktemp("sdp_e2e")
    src_df = StreamingSource("PYSPARK").write_to_json(tmp_path / "brz_source")
    pl = _get_pl_e2e(tmp_path)

    # Module scope: `monkeypatch` is function-scoped, patch for the run only
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(settings, "runtime_root", str(tmp_path))
        pl.orchestrator.execute(read_output=True)

    return SimpleNamespace(pl=pl, nodes=pl.nodes_dict, src_df=src_df.to_native())


def test_execute_transformers(e2e):
    """NATIVE, NARWHALS and SQL JOIN transformers: output values for each node"""
    nd = e2e.nodes

    # brz: NATIVE PySpark transformer adds zero=0.0
    df_brz = e2e.src_df.withColumn("zero", F.lit(0.0))
    assert_dfs_equal(nd["brz"].output_df, df_brz)

    # slv: NATIVE PySpark transformer adds x2=x1
    df_slv = df_brz.withColumn("x2", F.col("x1"))
    assert_dfs_equal(nd["slv"].output_df, df_slv)

    # slv_nw: NARWHALS transformer adds y1=x1 (same value as x2 - different API)
    expected_slv_nw = nw.from_native(df_brz).with_columns(y1=nw.col("x1"))
    assert_dfs_equal(nd["slv_nw"].output_df, expected_slv_nw)

    # gld_join: SQL LEFT JOIN of slv (primary) and slv_nw on id
    # result columns: id, x2 (from slv), y1 (from slv_nw); x2 == y1 == x1
    expected_gld = (
        nw.from_native(df_slv).select("id", "x2").with_columns(y1=nw.col("x2"))
    )
    assert_dfs_equal(nd["gld_join"].output_df, expected_gld)

    # gld_join covers the PARQUET sink format
    warehouse = e2e.pl.root_path.absolute() / "spark-warehouse"
    assert (warehouse / "gld_join").exists()
    assert not (warehouse / "gld_join" / "_delta_log").exists()


def test_is_sdp_execute_true_during_execution(e2e):
    """
    Verifies that is_sdp_execute() returns True inside the spark-pipelines subprocess.

    laktory_sdp.py writes the return value of is_sdp_execute() to
    .laktory_is_sdp_execute in root_path immediately after reading the pipeline
    config.  Asserting that file contains "true" is a direct, robust check that
    is independent of is_sdp_compatible or any expectation logic.
    """
    sentinel = e2e.pl.root_path / ".laktory_is_sdp_execute"
    assert sentinel.exists(), "laktory_sdp.py did not write the sentinel file"
    assert sentinel.read_text().strip() == "true"


def test_execute_declarative(e2e):
    """
    Declarative datasets: batch materialized_view, streaming table, temporary view.
    Output values verified for the batch node; streaming table row count verified.
    """
    nd = e2e.nodes

    # dec_slv: pass-through node, output equals brz
    df_brz = e2e.src_df.withColumn("zero", F.lit(0.0))
    assert_dfs_equal(nd["dec_slv"].output_df, df_brz)

    # dec_slv_stream: streaming table accumulates 3 rows from one run
    assert nw.from_native(nd["dec_slv_stream"].output_df).collect().shape[0] == 3

    # dec_gld_view: temporary pipeline view - not persisted to warehouse
    assert nd["dec_gld_view"].output_df is None

    # Tables default to Delta, as on Databricks
    warehouse = e2e.pl.root_path.absolute() / "spark-warehouse"
    for table in ["brz", "dec_slv", "dec_slv_stream"]:
        assert (warehouse / table / "_delta_log").exists()


def test_execute_shared_sink(e2e):
    """Two streaming nodes append to the same Delta table through append flows"""
    warehouse = e2e.pl.root_path.absolute() / "spark-warehouse"
    assert (warehouse / "shared" / "_delta_log").exists()
    df = e2e.nodes["feed_a"].output_df
    assert nw.from_native(df).collect().shape[0] == 6


@pytest.mark.slow
@_skip_no_sdp
@pytest.mark.xfail(
    strict=False,
    reason=(
        "Local spark-pipelines runs use an in-memory catalog, so a second run tries to "
        "re-create existing streaming tables (LOCATION_ALREADY_EXISTS for Parquet, "
        "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION for Delta). With a Hive catalog, "
        "Delta re-runs fail with DELTA_CANNOT_CHANGE_PROVIDER (open-source SDP / Delta "
        "gap, still present with PySpark 4.2.0 and delta-spark 4.4.0). Passes on "
        "Databricks. See #680."
    ),
)
def test_streaming_incremental(tmp_path, monkeypatch, spark):
    """
    Streaming SDP table accumulates new rows across successive pipeline runs.
    Run 1: source has 3 rows → streaming table has 3 rows.
    Run 2: 3 more rows appended to Delta source → streaming table has 6 rows.

    Marked `slow` (two `spark-pipelines` runs, expected to fail locally): excluded
    from `make test`, run by a single CI matrix entry to catch an upstream fix.
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


# ---------------------------------------------------------------------------
# SDP dependency inference: batch -> batch node chain
# ---------------------------------------------------------------------------


def test_sdp_batch_to_batch_reuses_cached_output(monkeypatch):
    """
    Under the SDP runtime a batch->batch node chain must NOT switch to a
    spark.read.table() of the upstream dataset. The local `spark-pipelines`
    runtime eagerly analyzes each dataset's plan (Laktory's SQL transformer calls
    spark.sql), so referencing a not-yet-materialized pipeline dataset by name
    fails analysis at graph-registration time. SDP therefore keeps the in-memory
    short-circuit and inlines the upstream plan from the original source.

    (The declarative dataset-read path is reserved for Lakeflow/LDP - see
    test_orchestrator_lakeflow_declarative.py - where DLT resolves pipeline-dataset
    reads during analysis.)
    """
    import laktory
    from laktory.models.datasources.tabledatasource import TableDataSource

    # Simulate execution inside the SDP runtime
    monkeypatch.setattr(laktory, "is_sdp_execute", lambda: True)

    pl = models.Pipeline.model_validate(
        {
            "name": "pl-sdp-batch-chain",
            "orchestrator": _SDP_ORCH,
            "nodes": [
                {
                    "name": "brz",
                    "sources": [{"format": "JSON", "path": "/src/"}],
                    "sinks": [{"table_name": "brz", "format": "PARQUET"}],
                },
                {
                    "name": "slv",
                    "sources": [{"node_name": "brz"}],
                    "sinks": [{"table_name": "slv", "format": "PARQUET"}],
                },
                {
                    "name": "gld",
                    "sources": [{"node_name": "slv"}],
                    "sinks": [{"table_name": "gld", "format": "PARQUET"}],
                },
            ],
        }
    )
    slv = pl.nodes_dict["slv"]

    assert slv.has_streaming_source is False

    # Simulate slv already executed in the same interpreter (cached output_df)
    sentinel_cached = object()
    slv._output_df = sentinel_cached

    def fail_read_spark(self):
        raise AssertionError("SDP must not read the upstream dataset by name")

    monkeypatch.setattr(TableDataSource, "_read_spark", fail_read_spark)

    source = pl.nodes_dict["gld"].sources[0]
    df = source._read_spark()

    assert df is sentinel_cached
