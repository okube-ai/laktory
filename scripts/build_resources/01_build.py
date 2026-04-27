"""
Generates Pydantic *_base.py model files for Databricks resources directly from
`databricks_schema.json` (produced by `terraform providers schema -json`).

Usage:
    python scripts/build_resources/01_build.py
"""

from __future__ import annotations

import json
import keyword
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCHEMA_PATH = Path(__file__).parent / "databricks_schema.json"
DESCRIPTIONS_PATH = Path(__file__).parent / "databricks_descriptions.json"
OUTPUT_DIR = (
    Path(__file__).parent.parent.parent
    / "laktory"
    / "models"
    / "resources"
    / "databricks"
)
PROVIDER_KEY = "registry.terraform.io/databricks/databricks"

# Fields that are always excluded regardless of resource
# Also skips any attribute starting with "__" (internal Terraform provider fields)
ALWAYS_SKIP_ATTRS = {"id"}

# Field names reserved by BaseResource / BaseModel — must be renamed to avoid shadowing
# Format: {terraform_attr_name: python_field_name}
# The generated class will use `serialization_alias` so Terraform output is unchanged.
RESERVED_FIELD_RENAMES: dict[str, str] = {
    "options": "options_",
    "variables": "variables_",
    "lookup_existing": "lookup_existing_",
    "schema": "schema_",  # shadows Pydantic BaseModel.schema()
    "schema_json": "schema_json_",  # shadows Pydantic v1 BaseModel.schema_json()
    "validate": "validate_",  # shadows Pydantic BaseModel.validate() (v1 compat)
    "update": "update_",  # shadows Laktory BaseModel.update()
}

# Block types that are always excluded (Laktory infrastructure, not Databricks fields)
ALWAYS_SKIP_BLOCK_TYPES = {"provider_config"}

# Per-resource attribute exclusions: fields that override classes handle via
# @computed_field. If the base emits these as regular fields, Pydantic raises
# "override with a computed_field is incompatible".
PER_RESOURCE_SKIP_ATTRS: dict[str, set[str]] = {
    "databricks_alert": {"parent_path"},
    "databricks_dashboard": {"parent_path"},
    "databricks_dbfs_file": {"path"},
    "databricks_notebook": {"path"},
    "databricks_quality_monitor": {
        "table_name",
        "output_schema_name",
        # block_types used as single objects in override (TF marks them as list)
        "data_classification_config",
        "inference_log",
        "notifications",
        "schedule",
        "snapshot",
        "time_series",
    },
    "databricks_query": {"parent_path"},
    "databricks_workspace_file": {"path", "source"},
}

# Irregular singular→plural mappings for PluralField
IRREGULAR_PLURALS: dict[str, str] = {
    "library": "libraries",
    "init_scripts": "init_scripts",  # already plural; same form accepted
    "ssh_public_keys": "ssh_public_keys",  # already plural
    "cluster_mount_info": "cluster_mount_infos",
    "on_success": "on_successes",
    "on_duration_warning_threshold_exceeded": "on_duration_warning_threshold_exceededs",
}


# Maps a resource_key to an alternate naming key used for class/file naming.
# The terraform_resource_type property still returns the original resource_key.
RESOURCE_NAME_OVERRIDES: dict[str, str] = {
    "databricks_sql_table": "databricks_table",
}


def pluralize(name: str) -> str:
    """Derive plural form of a field name. Returns explicit mapping or name + 's'."""
    return IRREGULAR_PLURALS.get(name, name if name.endswith("s") else name + "s")


# Default generation targets
DEFAULT_TARGETS = [
    "databricks_access_control_rule_set",
    "databricks_alert",
    "databricks_app",
    "databricks_catalog",
    "databricks_cluster",
    "databricks_cluster_policy",
    "databricks_current_user",
    "databricks_dashboard",
    "databricks_dbfs_file",
    "databricks_directory",
    "databricks_external_location",
    "databricks_grant",
    "databricks_grants",
    "databricks_group",
    "databricks_group_member",
    "databricks_job",
    "databricks_metastore",
    "databricks_metastore_assignment",
    "databricks_metastore_data_access",
    "databricks_mlflow_experiment",
    "databricks_mlflow_model",
    "databricks_mlflow_webhook",
    "databricks_mws_ncc_binding",
    "databricks_mws_network_connectivity_config",
    "databricks_mws_permission_assignment",
    "databricks_notebook",
    "databricks_notification_destination",
    "databricks_obo_token",
    "databricks_permissions",
    "databricks_pipeline",
    "databricks_quality_monitor",
    "databricks_query",
    "databricks_recipient",
    "databricks_repo",
    "databricks_schema",
    "databricks_secret",
    "databricks_secret_acl",
    "databricks_secret_scope",
    "databricks_service_principal",
    "databricks_service_principal_role",
    "databricks_share",
    "databricks_sql_endpoint",  # Warehouse
    "databricks_sql_table",
    "databricks_storage_credential",
    "databricks_user",
    "databricks_user_role",
    "databricks_vector_search_endpoint",
    "databricks_vector_search_index",
    "databricks_volume",
    "databricks_workspace_binding",
    "databricks_workspace_file",
]


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


def tf_type_to_python(tf_type) -> str:
    """Convert a Terraform type descriptor to a Python type annotation string."""
    if isinstance(tf_type, str):
        mapping = {
            "string": "str",
            "bool": "bool",
            "number": "float",
            "dynamic": "Any",
        }
        return mapping.get(tf_type, "Any")

    if isinstance(tf_type, list):
        container, inner = tf_type[0], tf_type[1]
        if container in ("list", "set"):
            return f"list[{tf_type_to_python(inner)}]"
        if container == "map":
            return f"dict[str, {tf_type_to_python(inner)}]"
        if container == "object":
            # inline object — use dict rather than a named model
            return "dict[str, Any]"

    return "Any"


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def resource_to_class_name(resource_key: str) -> str:
    """databricks_catalog -> Catalog"""
    name = resource_key.removeprefix("databricks_")
    return "".join(part.capitalize() for part in name.split("_"))


def block_class_name(parent_class: str, block_key: str) -> str:
    """Catalog + effective_predictive_optimization_flag -> CatalogEffectivePredictiveOptimizationFlag"""
    suffix = "".join(part.capitalize() for part in block_key.split("_"))
    return f"{parent_class}{suffix}"


def safe_field_name(name: str) -> str:
    """Rename fields that conflict with BaseResource/BaseModel, then escape keywords."""
    if name in RESERVED_FIELD_RENAMES:
        return RESERVED_FIELD_RENAMES[name]
    if keyword.iskeyword(name):
        return f"{name}_"
    return name


def needs_alias(name: str) -> bool:
    """True if the field was renamed from its Terraform name and needs serialization_alias."""
    return name in RESERVED_FIELD_RENAMES


# ---------------------------------------------------------------------------
# Attribute classification
# ---------------------------------------------------------------------------


def is_computed_only(attr: dict) -> bool:
    """True if the field is set by the provider and not user-settable."""
    return (
        attr.get("computed", False)
        and not attr.get("optional", False)
        and not attr.get("required", False)
    )


def field_default(attr: dict) -> str:
    """Return the Field(...) default expression for an attribute."""
    if attr.get("required"):
        return "..."
    return "None"


# ---------------------------------------------------------------------------
# Code emitters
# ---------------------------------------------------------------------------


def get_desc(
    descriptions: dict[str, str],
    block_path: str,
    attr_name: str,
    attr: dict,
) -> str:
    """
    Resolve a field description using a three-level priority chain:
      1. Path-qualified key  (e.g. "parameter.name")
      2. Bare key            (e.g. "name")
      3. TF schema text      (attr["description"])

    block_path is the dotted path from the resource root to the *current*
    block, e.g. "" for top-level, "parameter" one level in,
    "task.webhook_notifications.on_failure" deeper still.
    """
    qualified = f"{block_path}.{attr_name}" if block_path else attr_name
    raw = (
        descriptions.get(qualified)
        or descriptions.get(attr_name)
        or attr.get("description", "")
    )
    return raw.strip().replace('"', "'")


def emit_block_class(
    class_name: str,
    block: dict,
    descriptions: dict[str, str],
    block_path: str = "",
    indent: int = 0,
) -> tuple[list[str], list[str], list[str]]:
    """
    Recursively emit a nested block as a Pydantic BaseModel class.

    Returns (nested_classes, field_lines, all_class_names) where nested_classes
    contains companion class code that must be emitted before this class and
    all_class_names is the list of every class name defined (including nested ones).
    """
    pre_classes: list[str] = []
    lines: list[str] = []
    all_class_names: list[str] = [class_name]

    lines.append(f"class {class_name}(BaseModel):")

    attrs = block.get("attributes", {})
    block_types = block.get("block_types", {})
    has_fields = False

    # Emit attributes
    for attr_name, attr in sorted(attrs.items()):
        if attr_name in ALWAYS_SKIP_ATTRS or attr_name.startswith("__"):
            continue
        if is_computed_only(attr):
            continue

        py_type = tf_type_to_python(attr.get("type", "dynamic"))
        default = field_default(attr)
        description = get_desc(descriptions, block_path, attr_name, attr)
        field_name = safe_field_name(attr_name)

        if default == "...":
            type_str = py_type
        else:
            type_str = f"{py_type} | None"

        desc_snippet = f', description="{description}"' if description else ""
        if needs_alias(attr_name):
            alias_snippet = (
                f', serialization_alias="{attr_name}", '
                f'validation_alias=AliasChoices("{attr_name}", "{field_name}")'
            )
        else:
            alias_snippet = ""
        lines.append(
            f"    {field_name}: {type_str} = Field({default}{desc_snippet}{alias_snippet})"
        )
        has_fields = True

    # Emit block_type fields (nested models)
    for bt_name, bt_def in sorted(block_types.items()):
        if bt_name in ALWAYS_SKIP_BLOCK_TYPES:
            continue

        nested_path = f"{block_path}.{bt_name}" if block_path else bt_name
        nested_class = block_class_name(class_name, bt_name)
        nested_pre, nested_lines, nested_names = emit_block_class(
            nested_class, bt_def["block"], descriptions, block_path=nested_path
        )
        pre_classes.extend(nested_pre)
        pre_classes.append("\n".join(nested_lines))
        pre_classes.append("")  # blank line between classes
        all_class_names.extend(nested_names)

        nesting_mode = bt_def.get("nesting_mode", "list")
        max_items = bt_def.get("max_items", None)

        if max_items == 1 or nesting_mode == "single":
            type_str = f"{nested_class} | None"
            default = "None"
        else:
            type_str = f"list[{nested_class}] | None"
            default = "None"

        bt_field_name = safe_field_name(bt_name)
        desc = get_desc(descriptions, block_path, bt_name, {})
        desc_snippet = f', description="{desc}"' if desc else ""
        plural = pluralize(bt_field_name)
        if type_str.startswith("list[") and bt_field_name != plural:
            lines.append(
                f'    {bt_field_name}: {type_str} = PluralField({default}, plural="{plural}"{desc_snippet})'
            )
        else:
            lines.append(
                f"    {bt_field_name}: {type_str} = Field({default}{desc_snippet})"
            )
        has_fields = True

    if not has_fields:
        lines.append("    pass")

    return pre_classes, lines, all_class_names


def emit_resource_module(
    resource_key: str,
    resource_schema: dict,
    descriptions: dict[str, str],
    name_key: str | None = None,
) -> str:
    """
    Generate the full content of a Python module for a Databricks resource.
    """
    naming_key = name_key or resource_key
    class_name = resource_to_class_name(naming_key)
    block = resource_schema["block"]
    attrs = block.get("attributes", {})
    block_types = block.get("block_types", {})

    # Determine if Any is needed
    needs_any = any(
        "Any" in tf_type_to_python(a.get("type", "dynamic"))
        for a in attrs.values()
        if not is_computed_only(a) and a.get("name") not in ALWAYS_SKIP_ATTRS
    )

    per_resource_skip = PER_RESOURCE_SKIP_ATTRS.get(resource_key, set())

    # --- Build nested classes ---
    pre_classes: list[str] = []
    nested_field_lines: list[str] = []
    all_class_names: list[str] = []

    for bt_name, bt_def in sorted(block_types.items()):
        if bt_name in ALWAYS_SKIP_BLOCK_TYPES:
            continue
        if bt_name in per_resource_skip:
            continue

        nested_class = block_class_name(class_name, bt_name)
        nested_pre, nested_lines, nested_names = emit_block_class(
            nested_class, bt_def["block"], descriptions, block_path=bt_name
        )
        pre_classes.extend(nested_pre)
        pre_classes.append("\n".join(nested_lines))
        pre_classes.append("")
        all_class_names.extend(nested_names)

        nesting_mode = bt_def.get("nesting_mode", "list")
        max_items = bt_def.get("max_items", None)

        if max_items == 1 or nesting_mode == "single":
            type_str = f"{nested_class} | None"
            default_val = "None"
        else:
            type_str = f"list[{nested_class}] | None"
            default_val = "None"

        bt_field_name = safe_field_name(bt_name)
        desc = get_desc(descriptions, "", bt_name, {})
        desc_snippet = f', description="{desc}"' if desc else ""
        plural = pluralize(bt_field_name)
        if type_str.startswith("list[") and plural != bt_field_name:
            nested_field_lines.append(
                f'    {bt_field_name}: {type_str} = PluralField({default_val}, plural="{plural}"{desc_snippet})'
            )
        else:
            nested_field_lines.append(
                f"    {bt_field_name}: {type_str} = Field({default_val}{desc_snippet})"
            )

    # --- Build main class ---
    all_class_names.append(f"{class_name}Base")
    main_lines: list[str] = []
    main_lines.append(f"class {class_name}Base(BaseModel, TerraformResource):")
    main_lines.append('    """')
    main_lines.append(f"    Generated base class for `{resource_key}`.")
    main_lines.append(
        "    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`."
    )
    main_lines.append('    """')
    main_lines.append("")
    main_lines.append("    __doc_generated_base__ = True")
    main_lines.append("")

    has_fields = False

    # Required fields first
    required_attrs = {
        k: v
        for k, v in attrs.items()
        if v.get("required")
        and k not in ALWAYS_SKIP_ATTRS
        and not k.startswith("__")
        and k not in per_resource_skip
    }
    optional_attrs = {
        k: v
        for k, v in attrs.items()
        if not v.get("required")
        and not is_computed_only(v)
        and k not in ALWAYS_SKIP_ATTRS
        and not k.startswith("__")
        and k not in per_resource_skip
    }

    for attr_name, attr in sorted(required_attrs.items()):
        py_type = tf_type_to_python(attr.get("type", "dynamic"))
        description = get_desc(descriptions, "", attr_name, attr)
        field_name = safe_field_name(attr_name)
        desc_snippet = f', description="{description}"' if description else ""
        if needs_alias(attr_name):
            alias_snippet = (
                f', serialization_alias="{attr_name}", '
                f'validation_alias=AliasChoices("{attr_name}", "{field_name}")'
            )
        else:
            alias_snippet = ""

        plural = pluralize(field_name)
        if py_type.startswith("list[") and field_name != plural:
            main_lines.append(
                f'    {field_name}: {py_type} = PluralField(..., plural="{plural}"{desc_snippet}{alias_snippet})'
            )
        else:
            main_lines.append(
                f"    {field_name}: {py_type} = Field(...{desc_snippet}{alias_snippet})"
            )
        has_fields = True

    for attr_name, attr in sorted(optional_attrs.items()):
        py_type = tf_type_to_python(attr.get("type", "dynamic"))
        description = get_desc(descriptions, "", attr_name, attr)
        field_name = safe_field_name(attr_name)
        desc_snippet = f', description="{description}"' if description else ""
        if needs_alias(attr_name):
            alias_snippet = (
                f', serialization_alias="{attr_name}", '
                f'validation_alias=AliasChoices("{attr_name}", "{field_name}")'
            )
        else:
            alias_snippet = ""
        plural = pluralize(field_name)
        if py_type.startswith("list[") and plural != field_name:
            main_lines.append(
                f'    {field_name}: {py_type} | None = PluralField(None, plural="{plural}"{desc_snippet}{alias_snippet})'
            )
        else:
            main_lines.append(
                f"    {field_name}: {py_type} | None = Field(None{desc_snippet}{alias_snippet})"
            )
        has_fields = True

    # Nested block fields
    main_lines.extend(nested_field_lines)
    if nested_field_lines:
        has_fields = True

    if not has_fields:
        main_lines.append("    pass")

    main_lines.append("")
    main_lines.append("    @property")
    main_lines.append("    def terraform_resource_type(self) -> str:")
    main_lines.append(f'        return "{resource_key}"')

    # --- Assemble module ---
    body_parts: list[str] = []
    if pre_classes:
        body_parts.append("\n".join(pre_classes))
        body_parts.append("")
    body_parts.append("\n".join(main_lines))
    body = "\n".join(body_parts)

    any_import = "Any, " if needs_any else ""
    alias_choices_import = ", AliasChoices" if "AliasChoices(" in body else ""
    header = textwrap.dedent(f"""\
        # GENERATED FILE — DO NOT EDIT
        # Regenerate with: python scripts/build_resources/01_build.py {resource_key}
        from __future__ import annotations

        from typing import {any_import}Union

        from pydantic import Field{alias_choices_import}

        from laktory.models.basemodel import BaseModel, PluralField
        from laktory.models.resources.terraformresource import TerraformResource

    """)

    all_str = ", ".join(f'"{n}"' for n in all_class_names)
    all_decl = f"__all__ = [{all_str}]"

    parts = [header, body, all_decl, ""]

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    import subprocess

    schema = json.loads(SCHEMA_PATH.read_text())
    resource_schemas = schema["provider_schemas"][PROVIDER_KEY]["resource_schemas"]

    all_descriptions: dict[str, dict[str, str]] = {}
    if DESCRIPTIONS_PATH.exists():
        all_descriptions = json.loads(DESCRIPTIONS_PATH.read_text())
    else:
        print(
            f"[WARN] {DESCRIPTIONS_PATH.name} not found — run scripts/build_resources/00_fetch.py to add field descriptions"
        )

    out_dir = OUTPUT_DIR
    written: list[Path] = []
    written_keys: list[str] = []

    for resource_key in DEFAULT_TARGETS:
        if resource_key not in resource_schemas:
            print(f"[SKIP] {resource_key} — not found in schema")
            continue

        descriptions = all_descriptions.get(resource_key, {})
        naming_key = RESOURCE_NAME_OVERRIDES.get(resource_key, resource_key)
        code = emit_resource_module(
            resource_key,
            resource_schemas[resource_key],
            descriptions,
            name_key=naming_key,
        )
        fname = f"{naming_key.removeprefix('databricks_').replace('_', '')}_base.py"
        out_path = out_dir / fname
        out_path.write_text(code)
        written.append(out_path)
        written_keys.append(resource_key)
        print(
            f"[OK]   {resource_key} -> {out_path.relative_to(Path(__file__).parent.parent.parent)}"
        )

    if written:
        sys.stdout.flush()
        str_paths = [str(p) for p in written]
        subprocess.run(["ruff", "format"] + str_paths, check=True)
        subprocess.run(["ruff", "check", "--fix"] + str_paths, check=True)
        print("[OK]   ruff format + check applied")

        update_api = Path(__file__).parent / "02_update_api.py"
        subprocess.run([sys.executable, str(update_api)] + written_keys, check=True)


if __name__ == "__main__":
    main()
