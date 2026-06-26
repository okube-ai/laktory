"""Tests for the Laktory MCP server business logic (no mcp package required)."""

from laktory.mcp._model_docs import get_laktory_docs
from laktory.mcp._model_docs import get_model_docs
from laktory.mcp._model_docs import list_models
from laktory.mcp._validate import validate_yaml


def test_get_model_docs_pipelinenode():
    docs = get_model_docs("PipelineNode")
    assert "| `name`" in docs
    assert "| `sources`" in docs
    assert "| `sinks`" in docs
    assert "_pipeline" not in docs
    # alias fields must use user-facing name (no trailing _)
    assert "execution_task_name_" not in docs
    assert "| `execution_task_name`" in docs


def test_get_model_docs_type_rendering():
    docs = get_model_docs("PipelineNode")
    # name field should show 'str', not 'Union'
    assert "Union" not in docs
    # sources field should show list[...], not a bare type
    assert "list[" in docs


def test_get_model_docs_alias():
    docs = get_model_docs("FileDataSource")
    # AliasChoices with two user-facing names: both must appear
    assert "| `schema`" in docs
    assert "| `schema_definition`" in docs
    # AliasChoices with one user-facing name: internal backing name excluded
    assert "| `schema_location`" in docs
    assert "schema_location_" not in docs


def test_get_model_docs_plural_field():
    docs = get_model_docs("Job")
    # PluralField: singular (Python name) and plural (alias) must both appear
    assert "| `task`" in docs
    assert "| `tasks`" in docs
    assert "| `library`" in docs
    assert "| `libraries`" in docs


def test_get_model_docs_case_insensitive():
    docs_lower = get_model_docs("pipelinenode")
    docs_mixed = get_model_docs("PipelineNode")
    assert docs_lower == docs_mixed


def test_get_model_docs_unknown():
    result = get_model_docs("DoesNotExist")
    assert "not found" in result.lower()
    assert "Pipeline" in result or "PipelineNode" in result


def test_list_models_categories():
    models = list_models()
    for category in (
        "pipeline",
        "sources",
        "sinks",
        "orchestrators",
        "stack",
        "resources",
    ):
        assert category in models


def test_list_models_core_models():
    models = list_models()
    all_models = [m for names in models.values() for m in names]
    for name in (
        "Pipeline",
        "PipelineNode",
        "UnityCatalogDataSource",
        "UnityCatalogDataSink",
        "Stack",
    ):
        assert name in all_models
    assert len(all_models) >= 20


def test_list_models_resource_models():
    models = list_models()
    resources = models["resources"]
    for name in ("Catalog", "Group", "User", "Cluster", "Warehouse", "Job"):
        assert name in resources


def test_get_model_docs_resource_catalog():
    docs = get_model_docs("Catalog")
    assert "| `name`" in docs


def test_validate_yaml_valid_pipeline():
    yaml_content = """
name: brz_stock_prices
nodes:
- name: brz_stock_prices
  sources:
  - path: /Volumes/dev/sources/landing/events/
    format: JSON
"""
    result = validate_yaml(yaml_content)
    assert result == {"valid": True}


def test_validate_yaml_invalid_pipeline():
    yaml_content = """
nodes:
- sources:
  - path: /data/
    format: JSON
"""
    result = validate_yaml(yaml_content)
    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert any("name" in e for e in result["errors"])


def test_validate_yaml_bad_yaml():
    result = validate_yaml("key: [unclosed")
    assert result["valid"] is False
    assert any("YAML parse error" in e for e in result["errors"])


def test_get_version():
    from laktory._version import VERSION
    from laktory.mcp.server import get_version

    result = get_version()
    assert result == {"version": VERSION}


def test_get_laktory_docs():
    docs = get_laktory_docs()
    assert "Laktory" in docs
    assert "Pipeline" in docs
    assert len(docs) > 1000
