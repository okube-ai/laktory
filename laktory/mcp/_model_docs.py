from __future__ import annotations

import importlib
import types
import typing
from pathlib import Path
from typing import Any

from pydantic import AliasChoices
from pydantic import BaseModel

_AGENTS_MD = Path(__file__).parent.parent / "AGENTS.md"

_MODEL_REGISTRY: dict[str, list[str]] = {
    "pipeline": [
        "laktory.models.pipeline.pipeline:Pipeline",
        "laktory.models.pipeline.pipelinenode:PipelineNode",
        "laktory.models.dataframe.dataframetransformer:DataFrameTransformer",
        "laktory.models.dataframe.dataframeexpr:DataFrameExpr",
        "laktory.models.dataframe.dataframemethod:DataFrameMethod",
    ],
    "sources": [
        "laktory.models.datasources.filedatasource:FileDataSource",
        "laktory.models.datasources.unitycatalogdatasource:UnityCatalogDataSource",
        "laktory.models.datasources.hivemetastoredatasource:HiveMetastoreDataSource",
        "laktory.models.datasources.pipelinenodedatasource:PipelineNodeDataSource",
    ],
    "sinks": [
        "laktory.models.datasinks.filedatasink:FileDataSink",
        "laktory.models.datasinks.unitycatalogdatasink:UnityCatalogDataSink",
        "laktory.models.datasinks.hivemetastoredatasink:HiveMetastoreDataSink",
        "laktory.models.datasinks.pipelineviewdatasink:PipelineViewDataSink",
        "laktory.models.datasinks.mergecdcoptions:DataSinkMergeCDCOptions",
    ],
    "orchestrators": [
        "laktory.models.pipeline.orchestrators.lakeflowjoborchestrator:LakeflowJobOrchestrator",
        "laktory.models.pipeline.orchestrators.lakeflowdeclarativepipelineorchestrator:LakeflowDeclarativePipelineOrchestrator",
        "laktory.models.pipeline.orchestrators.sparkdeclarativepipelineorchestrator:SparkDeclarativePipelineOrchestrator",
        "laktory.models.pipeline.orchestrators.airfloworchestrator:AirflowOrchestrator",
    ],
    "stack": [
        "laktory.models.stacks.stack:Stack",
    ],
    "resources": [
        "laktory.models.resources.databricks.catalog:Catalog",
        "laktory.models.resources.databricks.schema:Schema",
        "laktory.models.resources.databricks.volume:Volume",
        "laktory.models.resources.databricks.externallocation:ExternalLocation",
        "laktory.models.resources.databricks.storagecredential:StorageCredential",
        "laktory.models.resources.databricks.group:Group",
        "laktory.models.resources.databricks.user:User",
        "laktory.models.resources.databricks.serviceprincipal:ServicePrincipal",
        "laktory.models.resources.databricks.metastore:Metastore",
        "laktory.models.resources.databricks.cluster:Cluster",
        "laktory.models.resources.databricks.clusterpolicy:ClusterPolicy",
        "laktory.models.resources.databricks.warehouse:Warehouse",
        "laktory.models.resources.databricks.job:Job",
        "laktory.models.resources.databricks.secretscope:SecretScope",
        "laktory.models.resources.databricks.workspacetree:WorkspaceTree",
    ],
}

try:
    from laktory.typing import VariableType as _VariableType
except ImportError:
    _VariableType = None


def _load_class(ref: str) -> type[BaseModel]:
    module_path, class_name = ref.rsplit(":", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _nice_type(ann: Any) -> str:
    if ann is None:
        return "any"
    if ann is type(None):
        return "None"

    origin = getattr(ann, "__origin__", None)
    args = getattr(ann, "__args__", None)

    is_union = origin is typing.Union or (
        hasattr(types, "UnionType") and isinstance(ann, types.UnionType)
    )

    if is_union and args:
        filtered = [a for a in args if a is not _VariableType]
        if len(filtered) == 1:
            return _nice_type(filtered[0])
        return " | ".join(_nice_type(a) for a in filtered)

    if origin is list and args:
        return f"list[{_nice_type(args[0])}]"

    if origin is dict and args and len(args) == 2:
        return f"dict[{_nice_type(args[0])}, {_nice_type(args[1])}]"

    name = getattr(ann, "__name__", None)
    if name:
        return name
    return str(ann)


def _display_name(name: str, field_info: Any) -> str:
    """Return the user-facing YAML field name (strips trailing _ alias backing)."""
    if name.endswith("_") and isinstance(field_info.validation_alias, AliasChoices):
        choices = field_info.validation_alias.choices
        if choices:
            first = choices[0]
            if isinstance(first, str) and not first.endswith("_"):
                return first
    return name


def _is_public(name: str) -> bool:
    return not name.startswith("_")


def get_laktory_docs() -> str:
    if _AGENTS_MD.exists():
        return _AGENTS_MD.read_text(encoding="utf-8")
    return "AGENTS.md not found in the installed package."


def list_models() -> dict[str, list[str]]:
    return {
        category: [ref.rsplit(":", 1)[1] for ref in refs]
        for category, refs in _MODEL_REGISTRY.items()
    }


def get_model_docs(model_name: str) -> str:
    for refs in _MODEL_REGISTRY.values():
        for ref in refs:
            cls_name = ref.rsplit(":", 1)[1]
            if cls_name.lower() == model_name.lower():
                cls = _load_class(ref)
                return _render_docs(cls)

    available = sorted(
        ref.rsplit(":", 1)[1] for refs in _MODEL_REGISTRY.values() for ref in refs
    )
    return f'Model "{model_name}" not found. Available models: {", ".join(available)}'


def _render_docs(cls: type[BaseModel]) -> str:
    lines = [f"## `{cls.__name__}`\n"]
    if cls.__doc__:
        doc = cls.__doc__.strip()
        if doc:
            lines.append(doc + "\n")

    lines.append("| Field | Type | Default | Required | Description |")
    lines.append("|-------|------|---------|----------|-------------|")

    seen: set[str] = set()
    for name, field in cls.model_fields.items():
        if not _is_public(name):
            continue
        display = _display_name(name, field)
        if display in seen:
            continue
        seen.add(display)

        type_str = _nice_type(field.annotation)
        required = "yes" if field.is_required() else "no"
        default = "" if field.is_required() else repr(field.default)
        description = field.description or ""
        lines.append(
            f"| `{display}` | `{type_str}` | {default} | {required} | {description} |"
        )

    return "\n".join(lines)
