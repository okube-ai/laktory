"""
Tests for DABs integration features added in feat/dab-integration branch:
  - laktory_build_root setting
  - PipelineConfigWorkspaceFile.source / build()
  - DatabricksPipelineOrchestrator.to_dab_resource()
  - DatabricksJobOrchestrator.to_dab_resource()
  - laktory.dabs.load_resources()
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
        # {tmp_path} placeholder must be replaced before YAML parsing
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


def test_stack_settings_applies_build_root(tmp_path):
    """laktory_build_root declared in stack settings propagates to global settings."""
    original = settings.laktory_build_root
    try:
        models.Stack(
            name="test",
            settings={"laktory_build_root": str(tmp_path)},
        )
        assert settings.laktory_build_root == str(tmp_path)
    finally:
        settings.laktory_build_root = original


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


@pytest.mark.xfail(
    reason=(
        "DatabricksJobOrchestrator.to_dab_resource() serializes task fields "
        "with Laktory names (e.g. 'depends_ons') that don't match the DABs "
        "SDK field names ('depends_on'). Nested terraform_renames are not yet "
        "applied recursively."
    ),
    strict=True,
)
def test_job_to_dab_resource_returns_job_type(monkeypatch, tmp_path):
    """to_dab_resource() returns a databricks.bundles Job instance."""
    from databricks.bundles.jobs import Job as DabsJob

    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    resource = _get_pl_job().orchestrator.to_dab_resource()
    assert isinstance(resource, DabsJob)


@pytest.mark.xfail(
    reason="Same field naming issue as test_job_to_dab_resource_returns_job_type.",
    strict=True,
)
def test_job_to_dab_resource_preserves_name(monkeypatch, tmp_path):
    """DABs Job resource retains the job name from the orchestrator."""
    monkeypatch.setattr(settings, "laktory_build_root", str(tmp_path))
    pl = _get_pl_job()
    resource = pl.orchestrator.to_dab_resource()
    assert resource.as_dict().get("name") == pl.orchestrator.name


# ---------------------------------------------------------------------------
# dabs.load_resources()
# ---------------------------------------------------------------------------

_STACK_YAML_DLT = """\
name: test-stack
settings:
  laktory_build_root: {build_root}
resources:
  pipelines:
    pl-stocks:
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

_STACK_YAML_WITH_VAR = """\
name: test-stack
settings:
  laktory_build_root: {build_root}
resources:
  pipelines:
    pl-stocks:
      name: pl-${{var.env}}
      orchestrator:
        type: DATABRICKS_PIPELINE
        catalog: ${{var.env}}
        schema: sandbox
      nodes:
        - name: brz_stocks
          source:
            table_name: samples.nyctaxi.trips
          sinks:
            - table_name: brz_stocks
"""

_STACK_YAML_LAKTORY_PRIORITY = """\
name: test-stack
settings:
  laktory_build_root: {build_root}
variables:
  env: prod
resources:
  pipelines:
    pl-stocks:
      name: pl-${{var.env}}
      orchestrator:
        type: DATABRICKS_PIPELINE
        catalog: ${{var.env}}
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
    bundle.variables = {}
    return bundle


_FAKE_WORKSPACE_ROOT = "/Workspace/Users/test/.bundle/myapp/dev"


def test_load_resources_pipeline_count(tmp_path, mock_bundle):
    """load_resources() returns one pipeline per DLT orchestrator in the stack."""
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_DLT.format(build_root=str(tmp_path)))
    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
    }

    resources = load_resources(mock_bundle)
    assert len(resources.pipelines) == 1


def test_load_resources_writes_config_json(tmp_path, mock_bundle):
    """load_resources() writes the pipeline config JSON to laktory_build_root/pipelines/."""
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_DLT.format(build_root=str(tmp_path)))
    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
    }

    load_resources(mock_bundle)

    config = tmp_path / "pipelines" / "pl-stocks.json"
    assert config.exists()
    with config.open() as f:
        data = json.load(f)
    assert data.get("name") == "pl-stocks"


def test_load_resources_bundle_vars_injected(tmp_path, mock_bundle):
    """Bundle variables are available for variable substitution in the stack."""
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_WITH_VAR.format(build_root=str(tmp_path)))
    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
        "env": "dev",
    }

    resources = load_resources(mock_bundle)
    # Pipeline name resolved using bundle var env=dev → "pl-dev"
    assert len(resources.pipelines) == 1
    config = tmp_path / "pipelines" / "pl-dev.json"
    assert config.exists()


def test_load_resources_laktory_vars_take_priority(tmp_path, mock_bundle):
    """Laktory stack variables override bundle variables of the same name."""
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_LAKTORY_PRIORITY.format(build_root=str(tmp_path)))
    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
        "env": "dev",  # bundle says dev, stack says prod
    }

    resources = load_resources(mock_bundle)
    # Laktory stack variable env=prod wins → "pl-prod"
    assert len(resources.pipelines) == 1
    config = tmp_path / "pipelines" / "pl-prod.json"
    assert config.exists()


# ---------------------------------------------------------------------------
# Auto-configure laktory_build_root and workspace_laktory_root
# ---------------------------------------------------------------------------

_STACK_YAML_NO_SETTINGS = """\
name: test-stack
resources:
  pipelines:
    pl-stocks:
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

_STACK_YAML_WITH_BUILD_ROOT = """\
name: test-stack
settings:
  laktory_build_root: {build_root}
resources:
  pipelines:
    pl-stocks:
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


def test_build_root_auto_set(tmp_path, mock_bundle, monkeypatch):
    """When laktory_build_root is at its default, load_resources() sets it to {cwd}/laktory/.build."""
    from laktory._settings import settings
    from laktory.dabs import load_resources

    # Ensure we're at the default (restore_settings fixture handles teardown)
    settings.laktory_build_root = DEFAULT_LAKTORY_BUILD_ROOT

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_NO_SETTINGS)

    fake_cwd = tmp_path / "bundle_root"
    fake_cwd.mkdir()
    monkeypatch.chdir(fake_cwd)

    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
    }

    load_resources(mock_bundle)

    assert settings.laktory_build_root == str(fake_cwd / "laktory" / ".build")


def test_build_root_stack_overrides_auto(tmp_path, mock_bundle, monkeypatch):
    """Stack setting for laktory_build_root takes priority over the auto-set default."""
    from laktory._settings import settings
    from laktory.dabs import load_resources

    settings.laktory_build_root = DEFAULT_LAKTORY_BUILD_ROOT

    custom_root = tmp_path / "my_custom_build"
    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(
        _STACK_YAML_WITH_BUILD_ROOT.format(build_root=str(custom_root))
    )

    fake_cwd = tmp_path / "bundle_root"
    fake_cwd.mkdir()
    monkeypatch.chdir(fake_cwd)

    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": _FAKE_WORKSPACE_ROOT,
    }

    load_resources(mock_bundle)

    assert settings.laktory_build_root == str(custom_root)
    assert settings.laktory_build_root != str(fake_cwd / "laktory" / ".build")


def test_workspace_root_auto_set(tmp_path, mock_bundle, monkeypatch):
    """workspace_laktory_root is derived from dab_workspace_root bundle variable, /Workspace/ prefix stripped."""
    from laktory._settings import settings
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_NO_SETTINGS)

    fake_cwd = tmp_path / "bundle_root"
    fake_cwd.mkdir()
    monkeypatch.chdir(fake_cwd)

    workspace_root = "/Workspace/Users/test/.bundle/myapp/dev"
    mock_bundle.variables = {
        "laktory_stack_filepath": str(stack_file),
        "dab_workspace_root": workspace_root,
    }

    load_resources(mock_bundle)

    # /Workspace/ is stripped so Laktory can prepend it consistently
    stripped_root = workspace_root.replace("/Workspace/", "/")
    expected_rel = "laktory/.build"
    assert settings.workspace_laktory_root == f"{stripped_root}/files/{expected_rel}/"


def test_workspace_root_no_bundle_var(tmp_path, mock_bundle, monkeypatch):
    """When dab_workspace_root bundle variable is absent, load_resources() raises ValueError."""
    from laktory.dabs import load_resources

    stack_file = tmp_path / "stack.yaml"
    stack_file.write_text(_STACK_YAML_NO_SETTINGS)

    fake_cwd = tmp_path / "bundle_root"
    fake_cwd.mkdir()
    monkeypatch.chdir(fake_cwd)

    mock_bundle.variables = {"laktory_stack_filepath": str(stack_file)}

    with pytest.raises(ValueError, match="dab_workspace_root"):
        load_resources(mock_bundle)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
