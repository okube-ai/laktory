"""
Approach A: Pure Python string generation from Terraform provider schema.

Generates Pydantic model files for Databricks resources directly from
`databricks_schema.json` (produced by `terraform providers schema -json`).

Usage:
    python approach_a_templates.py                  # generate all targets
    python approach_a_templates.py databricks_volume # generate one resource

Output lands in compare_output/*.py
"""

from __future__ import annotations

import json
import keyword
import os
import re
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCHEMA_PATH = Path(__file__).parent / "databricks_schema.json"
OUTPUT_DIR = Path(__file__).parent.parent / "laktory" / "models" / "resources" / "databricks"
PROVIDER_KEY = "registry.terraform.io/databricks/databricks"

# Fields that are always excluded regardless of resource
ALWAYS_SKIP_ATTRS = {"id"}

# Field names reserved by BaseResource / BaseModel — must be renamed to avoid shadowing
# Format: {terraform_attr_name: python_field_name}
# The generated class will use `serialization_alias` so Terraform output is unchanged.
RESERVED_FIELD_RENAMES: dict[str, str] = {
    "options": "options_",
    "variables": "variables_",
    "lookup_existing": "lookup_existing_",
}

# Block types that are always excluded (Laktory infrastructure, not Databricks fields)
ALWAYS_SKIP_BLOCK_TYPES = {"provider_config"}

# Irregular singular→plural mappings for PluralField
IRREGULAR_PLURALS: dict[str, str] = {
    "library": "libraries",
    "init_scripts": "init_scripts",  # already plural; same form accepted
    "ssh_public_keys": "ssh_public_keys",  # already plural
    "cluster_mount_info": "cluster_mount_infos",
    "on_success": "on_successes",
    "on_duration_warning_threshold_exceeded": "on_duration_warning_threshold_exceededs",
}


def pluralize(name: str) -> str:
    """Derive plural form of a field name. Returns explicit mapping or name + 's'."""
    return IRREGULAR_PLURALS.get(name, name + "s")


# Default generation targets
DEFAULT_TARGETS = [
    "databricks_volume",
    "databricks_catalog",
    "databricks_metastore",
    "databricks_schema",
    "databricks_cluster",
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
    return attr.get("computed", False) and not attr.get("optional", False) and not attr.get("required", False)


def field_default(attr: dict) -> str:
    """Return the Field(...) default expression for an attribute."""
    if attr.get("required"):
        return "..."
    return "None"


# ---------------------------------------------------------------------------
# Code emitters
# ---------------------------------------------------------------------------

def emit_block_class(class_name: str, block: dict, indent: int = 0) -> tuple[list[str], list[str]]:
    """
    Recursively emit a nested block as a Pydantic BaseModel class.

    Returns (nested_classes, field_lines) where nested_classes contains
    companion class code that must be emitted before this class.
    """
    pre_classes: list[str] = []
    lines: list[str] = []

    lines.append(f"class {class_name}(BaseModel):")

    attrs = block.get("attributes", {})
    block_types = block.get("block_types", {})
    has_fields = False

    # Emit attributes
    for attr_name, attr in sorted(attrs.items()):
        if attr_name in ALWAYS_SKIP_ATTRS:
            continue
        if is_computed_only(attr):
            continue

        py_type = tf_type_to_python(attr["type"])
        default = field_default(attr)
        description = attr.get("description", "").strip().replace('"', "'")
        field_name = safe_field_name(attr_name)

        if default == "...":
            type_str = py_type
        else:
            type_str = f"{py_type} | None"

        desc_snippet = f', description="{description[:80]}"' if description else ""
        alias_snippet = f', serialization_alias="{attr_name}"' if needs_alias(attr_name) else ""
        lines.append(f'    {field_name}: {type_str} = Field({default}{desc_snippet}{alias_snippet})')
        has_fields = True

    # Emit block_type fields (nested models)
    for bt_name, bt_def in sorted(block_types.items()):
        if bt_name in ALWAYS_SKIP_BLOCK_TYPES:
            continue

        nested_class = block_class_name(class_name, bt_name)
        nested_pre, nested_lines = emit_block_class(nested_class, bt_def["block"])
        pre_classes.extend(nested_pre)
        pre_classes.append("\n".join(nested_lines))
        pre_classes.append("")  # blank line between classes

        nesting_mode = bt_def.get("nesting_mode", "list")
        max_items = bt_def.get("max_items", None)

        if max_items == 1 or nesting_mode == "single":
            type_str = f"{nested_class} | None"
            default = "None"
        else:
            type_str = f"list[{nested_class}]"
            default = "None"

        bt_field_name = safe_field_name(bt_name)
        lines.append(f"    {bt_field_name}: {type_str} = Field({default})")
        has_fields = True

    if not has_fields:
        lines.append("    pass")

    return pre_classes, lines


def emit_resource_module(resource_key: str, resource_schema: dict) -> str:
    """
    Generate the full content of a Python module for a Databricks resource.
    """
    class_name = resource_to_class_name(resource_key)
    block = resource_schema["block"]
    attrs = block.get("attributes", {})
    block_types = block.get("block_types", {})

    # Determine if Any is needed
    needs_any = any(
        "Any" in tf_type_to_python(a["type"])
        for a in attrs.values()
        if not is_computed_only(a) and a.get("name") not in ALWAYS_SKIP_ATTRS
    )

    # --- Build nested classes ---
    pre_classes: list[str] = []
    nested_field_lines: list[str] = []

    for bt_name, bt_def in sorted(block_types.items()):
        if bt_name in ALWAYS_SKIP_BLOCK_TYPES:
            continue

        nested_class = block_class_name(class_name, bt_name)
        nested_pre, nested_lines = emit_block_class(nested_class, bt_def["block"])
        pre_classes.extend(nested_pre)
        pre_classes.append("\n".join(nested_lines))
        pre_classes.append("")

        nesting_mode = bt_def.get("nesting_mode", "list")
        max_items = bt_def.get("max_items", None)

        if max_items == 1 or nesting_mode == "single":
            type_str = f"{nested_class} | None"
            default_val = "None"
        else:
            type_str = f"list[{nested_class}] | None"
            default_val = "None"

        bt_field_name = safe_field_name(bt_name)
        if type_str.startswith("list["):
            plural = pluralize(bt_field_name)
            nested_field_lines.append(
                f'    {bt_field_name}: {type_str} = PluralField({default_val}, plural="{plural}")'
            )
        else:
            nested_field_lines.append(f"    {bt_field_name}: {type_str} = Field({default_val})")

    # --- Build main class ---
    main_lines: list[str] = []
    main_lines.append(f"class {class_name}Base(BaseModel, TerraformResource):")
    main_lines.append(f'    """')
    main_lines.append(f'    Generated base class for `{resource_key}`.')
    main_lines.append(f'    DO NOT EDIT — regenerate from `approach_a_templates.py`.')
    main_lines.append(f'    """')
    main_lines.append("")

    has_fields = False

    # Required fields first
    required_attrs = {k: v for k, v in attrs.items()
                      if v.get("required") and k not in ALWAYS_SKIP_ATTRS}
    optional_attrs = {k: v for k, v in attrs.items()
                      if not v.get("required") and not is_computed_only(v) and k not in ALWAYS_SKIP_ATTRS}

    for attr_name, attr in sorted(required_attrs.items()):
        py_type = tf_type_to_python(attr["type"])
        description = attr.get("description", "").strip().replace('"', "'")
        field_name = safe_field_name(attr_name)
        desc_snippet = f', description="{description[:100]}"' if description else ""
        alias_snippet = f', serialization_alias="{attr_name}"' if needs_alias(attr_name) else ""
        if py_type.startswith("list["):
            plural = pluralize(field_name)
            main_lines.append(f'    {field_name}: {py_type} = PluralField(..., plural="{plural}"{desc_snippet}{alias_snippet})')
        else:
            main_lines.append(f'    {field_name}: {py_type} = Field(...{desc_snippet}{alias_snippet})')
        has_fields = True

    for attr_name, attr in sorted(optional_attrs.items()):
        py_type = tf_type_to_python(attr["type"])
        description = attr.get("description", "").strip().replace('"', "'")
        field_name = safe_field_name(attr_name)
        desc_snippet = f', description="{description[:100]}"' if description else ""
        alias_snippet = f', serialization_alias="{attr_name}"' if needs_alias(attr_name) else ""
        if py_type.startswith("list["):
            plural = pluralize(field_name)
            main_lines.append(f'    {field_name}: {py_type} | None = PluralField(None, plural="{plural}"{desc_snippet}{alias_snippet})')
        else:
            main_lines.append(f'    {field_name}: {py_type} | None = Field(None{desc_snippet}{alias_snippet})')
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
    any_import = ", Any" if needs_any else ""
    header = textwrap.dedent(f"""\
        # GENERATED FILE — DO NOT EDIT
        # Regenerate with: python scripts/build_base_resources.py {resource_key}
        from __future__ import annotations

        from typing{any_import} import Union

        from pydantic import Field

        from laktory.models.basemodel import BaseModel, PluralField
        from laktory.models.resources.terraformresource import TerraformResource

    """)

    parts = [header]
    if pre_classes:
        parts.append("\n".join(pre_classes))
        parts.append("")
    parts.append("\n".join(main_lines))
    parts.append("")  # trailing newline

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    schema = json.loads(SCHEMA_PATH.read_text())
    resource_schemas = schema["provider_schemas"][PROVIDER_KEY]["resource_schemas"]

    out_dir = OUTPUT_DIR
    def filename(resource_key):
        return f"{resource_key.removeprefix('databricks_')}_base.py"

    for resource_key in DEFAULT_TARGETS:
        if resource_key not in resource_schemas:
            print(f"[SKIP] {resource_key} — not found in schema")
            continue

        code = emit_resource_module(resource_key, resource_schemas[resource_key])
        fname = filename(resource_key)

        out_path = out_dir / fname
        out_path.write_text(code)
        print(f"[OK]   {resource_key} -> {out_path.relative_to(Path(__file__).parent.parent.parent)}")


if __name__ == "__main__":
    main()
