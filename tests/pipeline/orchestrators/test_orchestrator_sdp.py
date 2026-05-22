import shutil
from pathlib import Path

import polars
import pytest
import yaml

from laktory import models
from laktory._settings import settings

data_dirpath = Path(__file__).parent.parent.parent / "data"


def is_sdp_available():
    return shutil.which("spark-pipelines") is not None


def get_pl_sdp():
    filepath = data_dirpath / "pl_sdp.yaml"
    with open(filepath) as fp:
        pl = models.Pipeline.model_validate_yaml(fp)
    return pl


def test_spark_declarative_pipeline():
    pl = get_pl_sdp()
    assert pl.is_orchestrator_sdp
    assert not pl.is_orchestrator_ldp
    assert pl.orchestrator.type == "SPARK_DECLARATIVE_PIPELINE"
    assert pl.orchestrator.storage == "file:///tmp/pl-sdp-checkpoints"


def test_spark_declarative_pipeline_build(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    pl = get_pl_sdp()
    spec_path = pl.orchestrator.build()

    pipelines_dir = tmp_path / "pipelines"
    assert (pipelines_dir / "pl-sdp.json").exists()
    assert (pipelines_dir / "sdp_laktory_pl.py").exists()
    assert spec_path == pipelines_dir / "pl-sdp-spec.yml"
    assert spec_path.exists()

    with open(spec_path) as fp:
        spec = yaml.safe_load(fp)

    assert spec["name"] == "pl-sdp"
    assert spec["storage"] == "file:///tmp/pl-sdp-checkpoints"
    assert spec["libraries"] == [{"glob": {"include": "sdp_laktory_pl.py"}}]
    assert spec["configuration"]["laktory.is_sdp_execute"] == "true"
    assert "config_filepath" in spec["configuration"]


def test_spark_declarative_pipeline_execute(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    pl = get_pl_sdp()
    mock_run = mocker.patch("subprocess.run")
    pl.execute()
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "spark-pipelines"
    assert cmd[1] == "run"
    assert "--spec" in cmd
    assert str(tmp_path / "pipelines" / "pl-sdp-spec.yml") in cmd


@pytest.mark.skipif(
    not is_sdp_available(),
    reason="spark-pipelines CLI not available (requires PySpark 4.1+)",
)
def test_execute(tmp_path, monkeypatch):
    brz_path = (data_dirpath / "brz.parquet").resolve()
    monkeypatch.setattr(settings, "build_root", str(tmp_path))

    pl = models.Pipeline.model_validate(
        {
            "name": "pl-sdp-exec",
            "orchestrator": {
                "type": "SPARK_DECLARATIVE_PIPELINE",
                "storage": f"file://{tmp_path}/checkpoints",
            },
            "nodes": [
                {
                    "name": "brz",
                    "source": {"format": "PARQUET", "path": str(brz_path)},
                    "sinks": [{"table_name": "brz"}],
                },
                {
                    "name": "slv",
                    "source": {"node_name": "brz"},
                    "transformer": {
                        "nodes": [
                            {
                                "func_name": "with_columns",
                                "func_kwargs": {"y1": "x1"},
                            }
                        ]
                    },
                    "sinks": [{"table_name": "slv"}],
                },
            ],
        }
    )

    pl.orchestrator.execute(cwd=str(tmp_path))

    df = polars.read_parquet(tmp_path / "spark-warehouse" / "slv").sort("id")
    assert df["id"].to_list() == ["a", "b", "c"]
    assert df["x1"].to_list() == [1, 2, 3]
    assert df["y1"].to_list() == [1, 2, 3]
