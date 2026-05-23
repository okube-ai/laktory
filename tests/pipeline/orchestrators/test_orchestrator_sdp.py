import io
import shutil
from pathlib import Path

import polars
import pytest
import yaml

from laktory import models
from laktory._settings import settings
from laktory._testing import StreamingSource

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


def test_spark_declarative_pipeline_build(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = get_pl_sdp(tmp_path)
    pl.orchestrator.build()

    root_dir = pl.root_path
    assert (root_dir / "pl-sdp.json").exists()
    assert (root_dir / "sdp_laktory_pl.py").exists()

    spec_path = pl.orchestrator.spec_filepath_abs
    assert spec_path == root_dir / "pl-sdp-spec.yaml"
    assert spec_path.exists()

    with open(spec_path) as fp:
        spec = yaml.safe_load(fp)

    assert spec["name"] == "pl-sdp"
    assert spec["schema"] == "default"
    assert spec["storage"] == "file://" + str(root_dir.absolute())
    assert spec["libraries"] == [{"glob": {"include": "sdp_laktory_pl.py"}}]
    assert spec["configuration"]["laktory.is_sdp_execute"] == "true"
    assert "config_filepath" in spec["configuration"]


def test_spark_declarative_pipeline_execute(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))
    pl = get_pl_sdp(tmp_path)
    mock_run = mocker.patch("subprocess.run")
    pl.execute()
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "spark-pipelines"
    assert cmd[1] == "run"
    assert "--spec" in cmd
    assert cmd[cmd.index("--spec") + 1] == "pl-sdp-spec.yaml"
    assert mock_run.call_args[1]["cwd"] == pl.root_path.absolute()


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute(tmp_path, monkeypatch, spark):
    monkeypatch.setattr(settings, "runtime_root", str(tmp_path))

    # Write bronze source
    ss = StreamingSource("PYSPARK")
    ss.write_to_json(tmp_path / "brz_source")

    # Get and execute pipeline
    pl = get_pl_sdp(tmp_path)
    pl.orchestrator.execute(cwd=str(tmp_path), read_output=True)

    # Test Results
    df = polars.read_parquet(tmp_path / "spark-warehouse" / "slv").sort("id")
    assert df["id"].to_list() == ["a", "b", "c"]
    assert df["x1"].to_list() == [1, 2, 3]
    assert df["y1"].to_list() == [1, 2, 3]
