import io
import shutil
from pathlib import Path

import pyspark.sql.functions as F
import pytest
import yaml

from laktory import models
from laktory._settings import settings
from laktory._testing import StreamingSource

from ...conftest import assert_dfs_equal

data_dirpath = Path(__file__).parent.parent.parent / "data"


def is_sdp_available():
    return shutil.which("spark-pipelines") is not None


def get_pl_sdp(tmp_path):
    filepath = data_dirpath / "pl_sdp.yaml"
    with open(filepath) as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
    return pl


def test_spark_declarative_pipeline(tmp_path):
    pl = get_pl_sdp(tmp_path)
    assert pl.is_orchestrator_sdp
    assert not pl.is_orchestrator_ldp
    assert pl.orchestrator.type == "SPARK_DECLARATIVE_PIPELINE"
    assert pl.orchestrator.schema_ == "default"
    assert pl.orchestrator.storage.startswith("file://")

    # schema_ from orchestrator is propagated to sinks that don't set it explicitly
    for node in pl.nodes:
        for sink in node.all_sinks:
            if isinstance(sink, models.TableDataSink):
                assert sink.schema_name == "default"


def test_spark_declarative_pipeline_build(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = get_pl_sdp(tmp_path)
    pl.orchestrator.build()

    root_dir = pl.root_path
    assert (root_dir / "pl-sdp.json").exists()
    assert (root_dir / "laktory_sdp.py").exists()

    spec_path = pl.orchestrator.spec_filepath_abs
    assert spec_path == root_dir / "spark-pipeline.yaml"
    assert spec_path.exists()

    with open(spec_path) as fp:
        spec = yaml.safe_load(fp)

    assert spec["name"] == "pl-sdp"
    assert spec["schema"] == "default"
    assert spec["storage"] == "file://" + str(root_dir.absolute())
    assert spec["libraries"] == [{"glob": {"include": "laktory_sdp.py"}}]
    assert spec["configuration"]["laktory.is_sdp_execute"] == "true"
    assert "config_filepath" in spec["configuration"]


def test_spark_declarative_pipeline_execute(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = get_pl_sdp(tmp_path)
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
    pl.execute(use_orchestrator=True, selects=["brz_prices", "slv_prices"])
    cmd = mock_run.call_args[0][0]
    assert "--refresh" in cmd
    assert cmd[cmd.index("--refresh") + 1] == "brz_prices,slv_prices"

    # full_refresh + selects → --full-refresh datasets
    mock_run.reset_mock()
    pl.execute(use_orchestrator=True, full_refresh=True, selects=["brz_prices"])
    cmd = mock_run.call_args[0][0]
    assert "--full-refresh" in cmd
    assert cmd[cmd.index("--full-refresh") + 1] == "brz_prices"


def test_spark_declarative_pipeline_incompatible_expectations(
    tmp_path, monkeypatch, spark
):
    import laktory

    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    monkeypatch.setattr(laktory, "is_sdp_execute", lambda: True)

    pl = get_pl_sdp(tmp_path)
    node = pl.nodes[0]

    # Provide a minimal _stage_df so check_expectations runs (skip real source read)
    node._stage_df = spark.createDataFrame([(1, 1.0)], ["id", "x1"])

    # Attach a non-SDP-compatible expectation
    node.expectations = [
        models.DataQualityExpectation(name="x1 positive", expr="x1 > 0")
    ]

    with pytest.raises(TypeError, match="not natively supported by SDP"):
        node.check_expectations()


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute(tmp_path, monkeypatch, spark):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    # Write bronze source
    ss = StreamingSource("PYSPARK")
    df0 = ss.write_to_json(tmp_path / "brz_source")
    df0 = df0.to_native()

    # Get and execute pipeline
    pl = get_pl_sdp(tmp_path)
    pl.orchestrator.execute(cwd=str(tmp_path), read_output=True)

    # Set targets
    df_brz = df0.withColumn("zero", F.lit(0.0))
    df_slv = df_brz.withColumn("x2", F.col("x1"))

    # Test Results
    assert_dfs_equal(pl.nodes[0].output_df, df_brz)
    assert_dfs_equal(pl.nodes[1].output_df, df_slv)
