"""
Tests for DABs integration features:
  - laktory_build_root setting
  - PipelineConfigWorkspaceFile.source / build()
  - DatabricksPipelineOrchestrator.to_dab_resource()
  - DatabricksJobOrchestrator.to_dab_resource()
  - laktory.dab.build_resources() — folder-scan approach
  - ${var.x} syntax support (DABs-style variable prefix)
"""

import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from laktory import models
from laktory._settings import DEFAULT_LAKTORY_BUILD_ROOT
from laktory._settings import Settings
from laktory._settings import settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

data_dir = Path(__file__).parent / "data"


def _get_pl_dlt():
    """Load DLT pipeline from pl_dlt.yaml."""
    filepath = data_dir / "pl_dlt.yaml"
    with open(filepath) as fp:
        data = fp.read().replace("{tmp_path}", "")
    return models.Pipeline.model_validate_yaml(io.StringIO(data))


def _get_pl_job():
    """Load Job pipeline from test helpers."""
    from tests.resources.test_pipeline_orchestrators import get_pl_job

    return get_pl_job()


# ---------------------------------------------------------------------------
# laktory_build_root setting
# ---------------------------------------------------------------------------


def test_laktory_build_root_default():
    """Default laktory_build_root equals DEFAULT_LAKTORY_BUILD_ROOT."""
    s = Settings()
    assert s.laktory_build_root == DEFAULT_LAKTORY_BUILD_ROOT


def test_laktory_build_root_env_var(monkeypatch, tmp_path):
    """LAKTORY_BUILD_ROOT environment variable overrides the default."""
    monkeypatch.setenv("LAKTORY_BUILD_ROOT", str(tmp_path))
    s = Settings()
    assert s.laktory_build_root == str(tmp_path)


# ---------------------------------------------------------------------------
# PipelineConfigWorkspaceFile
# ---------------------------------------------------------------------------


def test_config_file_source_uses_build_root(monkeypatch, tmp_path):
    """source property resolves under laktory_build_root/pipelines/."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    pl = _get_pl_dlt()
    expected = str(tmp_path / "pipelines" / f"{pl.name}.json")
    assert pl.orchestrator.config_file.source == expected


def test_config_file_build_writes_json(monkeypatch, tmp_path):
    """build() creates parent directories and writes a valid JSON config."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    pl = _get_pl_dlt()
    pl.orchestrator.config_file.build()

    out = tmp_path / "pipelines" / f"{pl.name}.json"
    assert out.exists()
    with out.open() as f:
        data = json.load(f)
    assert data.get("name") == pl.name


# ---------------------------------------------------------------------------
# DatabricksPipelineOrchestrator.to_dab_resource()
# ---------------------------------------------------------------------------


def test_dlt_to_dab_resource_returns_pipeline_type(monkeypatch, tmp_path):
    """to_dab_resource() returns a databricks.bundles Pipeline instance."""
    from databricks.bundles.pipelines import Pipeline as DabsPipeline

    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    (tmp_path / "pipelines").mkdir(parents=True, exist_ok=True)

    pl = _get_pl_dlt()
    resource = pl.orchestrator.to_dab_resource()
    assert isinstance(resource, DabsPipeline)


def test_dlt_to_dab_resource_copies_notebook(monkeypatch, tmp_path):
    """to_dab_resource() copies dlt_laktory_pl.py to laktory_build_root/pipelines/."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    (tmp_path / "pipelines").mkdir(parents=True, exist_ok=True)

    _get_pl_dlt().orchestrator.to_dab_resource()
    assert (tmp_path / "pipelines" / "dlt_laktory_pl.py").exists()


def test_dlt_to_dab_resource_notebook_path(monkeypatch, tmp_path):
    """Library notebook path in DABs resource points to workspace_laktory_root/pipelines/."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    (tmp_path / "pipelines").mkdir(parents=True, exist_ok=True)
    workspace_root = "/test/.laktory/"
    monkeypatch.setattr(settings, "workspace_laktory_root", workspace_root)

    resource = _get_pl_dlt().orchestrator.to_dab_resource()
    d = resource.as_dict()
    notebook_path = str(d["libraries"][0]["notebook"]["path"])
    assert notebook_path == f"/Workspace{workspace_root}pipelines/dlt_laktory_pl"


# ---------------------------------------------------------------------------
# DatabricksJobOrchestrator.to_dab_resource()
# ---------------------------------------------------------------------------


def test_job_to_dab_resource_returns_job_type(monkeypatch, tmp_path):
    """to_dab_resource() returns a databricks.bundles Job instance."""
    from databricks.bundles.jobs import Job as DabsJob

    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    resource = _get_pl_job().orchestrator.to_dab_resource()
    assert isinstance(resource, DabsJob)


def test_job_to_dab_resource_preserves_name(monkeypatch, tmp_path):
    """DABs Job resource retains the job name from the orchestrator."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    pl = _get_pl_job()
    resource = pl.orchestrator.to_dab_resource()
    assert resource.as_dict().get("name") == pl.orchestrator.name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_FAKE_WORKSPACE_ROOT = "/Workspace/Users/test/.bundle/myapp/dev"

# Minimal DLT pipeline YAML written to a pipelines directory
_PIPELINE_DLT_YAML = """\
name: pl-stocks
orchestrator:
  type: DATABRICKS_PIPELINE
  catalog: dev
  schema: sandbox
nodes:
  - name: brz_stocks
    source:
      table_name: samples.nyctaxi.trips
    sinks:
      - table_name: brz_stocks
"""

# Pipeline with a ${var.env} reference
_PIPELINE_WITH_VAR_YAML = """\
name: pl-${var.env}
orchestrator:
  type: DATABRICKS_PIPELINE
  catalog: ${var.env}
  schema: sandbox
nodes:
  - name: brz_stocks
    source:
      table_name: samples.nyctaxi.trips
    sinks:
      - table_name: brz_stocks
"""

# Pipeline with a pipeline-level variable (takes priority over bundle vars)
_PIPELINE_WITH_LAKTORY_VAR_YAML = """\
name: pl-${var.env}
variables:
  env: prod
orchestrator:
  type: DATABRICKS_PIPELINE
  catalog: ${var.env}
  schema: sandbox
nodes:
  - name: brz_stocks
    source:
      table_name: samples.nyctaxi.trips
    sinks:
      - table_name: brz_stocks
"""


@pytest.fixture(autouse=True)
def restore_settings():
    """Save and restore global settings around every test in this module."""
    original_build_root = settings.laktory_build_root
    original_workspace_root = settings.workspace_laktory_root
    yield
    settings.laktory_build_root = original_build_root
    settings.workspace_laktory_root = original_workspace_root


@pytest.fixture
def mock_bundle(tmp_path):
    """Minimal Bundle mock for load_resources."""
    bundle = MagicMock()
    bundle.variables = {
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
    }
    return bundle


def _make_pipelines_dir(tmp_path, yaml_content, filename="pl-stocks.yaml"):
    """Write a pipeline YAML to a pipelines/ subdirectory and return its path."""
    pipelines_dir = tmp_path / "laktory" / "pipelines"
    pipelines_dir.mkdir(parents=True)
    (pipelines_dir / filename).write_text(yaml_content)
    return pipelines_dir


# ---------------------------------------------------------------------------
# dabs.load_resources() — folder scan
# ---------------------------------------------------------------------------


def test_load_resources_pipeline_count(tmp_path, mock_bundle, monkeypatch):
    """load_resources() returns one pipeline per DLT orchestrator YAML file."""
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 1


def test_load_resources_writes_config_json(tmp_path, mock_bundle, monkeypatch):
    """load_resources() writes the pipeline config JSON to laktory_build_root/pipelines/."""
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)

    build_resources(mock_bundle)

    config = tmp_path / "laktory" / ".build" / "pipelines" / "pl-stocks.json"
    assert config.exists()
    with config.open() as f:
        data = json.load(f)
    assert data.get("name") == "pl-stocks"


def test_load_resources_bundle_vars_injected(tmp_path, mock_bundle, monkeypatch):
    """Bundle variables are substituted in pipeline fields."""
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_WITH_VAR_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)
    mock_bundle.variables["env"] = "dev"

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 1
    config = tmp_path / "laktory" / ".build" / "pipelines" / "pl-dev.json"
    assert config.exists()


def test_load_resources_laktory_vars_take_priority(tmp_path, mock_bundle, monkeypatch):
    """Pipeline-level variables override bundle variables of the same name."""
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_WITH_LAKTORY_VAR_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)
    mock_bundle.variables["env"] = "dev"  # bundle says dev, pipeline says prod

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 1
    config = tmp_path / "laktory" / ".build" / "pipelines" / "pl-prod.json"
    assert config.exists()


def test_load_resources_multiple_dirs(tmp_path, mock_bundle, monkeypatch):
    """Comma-separated laktory_pipelines_dir scans all listed directories."""
    from laktory.dab import build_resources

    dir_a = tmp_path / "pipelines_a"
    dir_b = tmp_path / "pipelines_b"
    dir_a.mkdir()
    dir_b.mkdir()
    (dir_a / "pl-a.yaml").write_text(_PIPELINE_DLT_YAML.replace("pl-stocks", "pl-a"))
    (dir_b / "pl-b.yaml").write_text(_PIPELINE_DLT_YAML.replace("pl-stocks", "pl-b"))

    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = f"{dir_a},{dir_b}"

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 2


def test_load_resources_missing_dir_skipped(tmp_path, mock_bundle, monkeypatch):
    """A non-existent pipelines directory is skipped with a warning, not an error."""
    from laktory.dab import build_resources

    nonexistent = tmp_path / "does_not_exist"
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(nonexistent)

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 0


def test_load_resources_default_dir(tmp_path, mock_bundle, monkeypatch):
    """When laktory_pipelines_dir is absent, load_resources() scans laktory/pipelines/."""
    from laktory.dab import build_resources

    pipelines_dir = tmp_path / "laktory" / "pipelines"
    pipelines_dir.mkdir(parents=True)
    (pipelines_dir / "pl-stocks.yaml").write_text(_PIPELINE_DLT_YAML)

    monkeypatch.chdir(tmp_path)
    # Do NOT set laktory_pipelines_dir — should use default

    resources = build_resources(mock_bundle)
    assert len(resources.pipelines) == 1


# ---------------------------------------------------------------------------
# Auto-configure laktory_build_root and workspace_laktory_root
# ---------------------------------------------------------------------------


def test_build_root_auto_set(tmp_path, mock_bundle, monkeypatch):
    """When laktory_build_root is at its default, load_resources() sets it to {cwd}/laktory/.build."""
    from laktory._settings import settings
    from laktory.dab import build_resources

    settings.laktory_build_root = DEFAULT_LAKTORY_BUILD_ROOT

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)

    build_resources(mock_bundle)

    assert settings.laktory_build_root == str(tmp_path / "laktory" / ".build")


def test_build_root_env_var_overrides_auto(tmp_path, mock_bundle, monkeypatch):
    """LAKTORY_BUILD_ROOT env var (via Settings) takes priority over the auto-set default."""
    from laktory._settings import settings
    from laktory.dab import build_resources

    custom_root = tmp_path / "my_custom_build"
    custom_root.mkdir()
    settings.laktory_build_root = str(custom_root)

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)

    build_resources(mock_bundle)

    assert settings.laktory_build_root == str(custom_root)
    assert settings.laktory_build_root != str(tmp_path / "laktory" / ".build")


def test_workspace_root_auto_set(tmp_path, mock_bundle, monkeypatch):
    """workspace_laktory_root is derived from dab_workspace_root, /Workspace/ prefix stripped."""
    from laktory._settings import settings
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables["laktory_pipelines_dir"] = str(pipelines_dir)

    workspace_root = "/Workspace/Users/test/.bundle/myapp/dev"
    mock_bundle.variables["dab_workspace_root"] = workspace_root

    build_resources(mock_bundle)

    stripped_root = workspace_root.replace("/Workspace/", "/")
    expected_rel = "laktory/.build"
    assert settings.workspace_laktory_root == f"{stripped_root}/files/{expected_rel}/"


def test_workspace_root_no_bundle_var(tmp_path, mock_bundle, monkeypatch):
    """When dab_workspace_root bundle variable is absent, load_resources() raises ValueError."""
    from laktory.dab import build_resources

    pipelines_dir = _make_pipelines_dir(tmp_path, _PIPELINE_DLT_YAML)
    monkeypatch.chdir(tmp_path)
    mock_bundle.variables = {"laktory_pipelines_dir": str(pipelines_dir)}

    with pytest.raises(ValueError, match="dab_workspace_root"):
        build_resources(mock_bundle)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
