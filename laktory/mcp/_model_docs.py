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


def _inner_model(ann: Any) -> type[BaseModel] | None:
    """Return the innermost Pydantic BaseModel class from a type annotation."""
    if ann is None or ann is type(None):
        return None

    origin = getattr(ann, "__origin__", None)
    args = getattr(ann, "__args__", None)

    is_union = origin is typing.Union or (
        hasattr(types, "UnionType") and isinstance(ann, types.UnionType)
    )

    if is_union and args:
        filtered = [a for a in args if a is not _VariableType and a is not type(None)]
        for a in filtered:
            result = _inner_model(a)
            if result is not None:
                return result
        return None

    if origin is list and args:
        return _inner_model(args[0])

    if origin is dict and args and len(args) == 2:
        return _inner_model(args[1])

    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return ann

    return None


def _all_nested_models(cls: type[BaseModel]) -> list[type[BaseModel]]:
    """Return all transitively reachable nested BaseModel types via BFS.

    Excludes registry models (they have their own get_model_docs entry).
    Deduplicates and preserves breadth-first discovery order.
    """
    registry_names = {
        ref.rsplit(":", 1)[1] for refs in _MODEL_REGISTRY.values() for ref in refs
    }
    seen: set[type] = {cls}
    queue: list[type[BaseModel]] = [cls]
    result: list[type[BaseModel]] = []

    while queue:
        current = queue.pop(0)
        for field in current.model_fields.values():
            inner = _inner_model(field.annotation)
            if (
                inner is not None
                and inner not in seen
                and inner.__name__ not in registry_names
            ):
                seen.add(inner)
                result.append(inner)
                queue.append(inner)

    return result


def _display_names(name: str, field_info: Any) -> list[str]:
    """Return all user-facing YAML names for a field.

    The Python field name comes first (if it is user-facing — no leading or
    trailing underscore), followed by any string AliasChoices entries that are
    also user-facing. Internal backing names (ending with _) and truly private
    names (starting with _) are excluded from both positions.
    """
    names: list[str] = []

    if not name.startswith("_") and not name.endswith("_"):
        names.append(name)

    if isinstance(field_info.validation_alias, AliasChoices):
        for c in field_info.validation_alias.choices:
            if isinstance(c, str) and not c.endswith("_") and c not in names:
                names.append(c)

    return names


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
        display_list = _display_names(name, field)
        if not display_list:
            continue

        type_str = _nice_type(field.annotation)
        required = "yes" if field.is_required() else "no"
        default = "" if field.is_required() else repr(field.default)
        description = field.description or ""

        for display in display_list:
            if display in seen:
                continue
            seen.add(display)
            lines.append(
                f"| `{display}` | `{type_str}` | {default} | {required} | {description} |"
            )

    for nested_cls in _all_nested_models(cls):
        lines.append(f"\n### `{nested_cls.__name__}`\n")
        if nested_cls.__doc__:
            doc = nested_cls.__doc__.strip()
            if doc:
                lines.append(doc + "\n")
        lines.append("| Field | Type | Default | Required | Description |")
        lines.append("|-------|------|---------|----------|-------------|")
        seen_nested: set[str] = set()
        for n_name, n_field in nested_cls.model_fields.items():
            n_display_list = _display_names(n_name, n_field)
            if not n_display_list:
                continue
            n_type_str = _nice_type(n_field.annotation)
            n_required = "yes" if n_field.is_required() else "no"
            n_default = "" if n_field.is_required() else repr(n_field.default)
            n_description = n_field.description or ""
            for n_display in n_display_list:
                if n_display in seen_nested:
                    continue
                seen_nested.add(n_display)
                lines.append(
                    f"| `{n_display}` | `{n_type_str}` | {n_default} | {n_required} | {n_description} |"
                )

    return "\n".join(lines)
