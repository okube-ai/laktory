"""
Field coverage comparison: generated fields vs hand-written Laktory models.

For each target resource, prints:
- Fields present in Terraform schema but absent from hand-written model (potential gaps)
- Fields present in hand-written model but absent from schema (Laktory extensions)

Run with:
    python field_coverage.py
"""

from __future__ import annotations

import ast
import importlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "../.."))

SCHEMA_PATH = Path(__file__).parent / "databricks_schema.json"
PROVIDER_KEY = "registry.terraform.io/databricks/databricks"
LAKTORY_RESOURCES_DIR = Path(__file__).parent / "../../laktory/models/resources/databricks"

# Map: terraform resource key -> laktory module name
TARGETS = {
    "databricks_volume": "volume",
    "databricks_catalog": "catalog",
    "databricks_metastore": "metastore",
    "databricks_schema": "schema",
}

# Fields always excluded from comparison (base class infrastructure)
BASE_FIELDS = {"resource_name_", "options", "lookup_existing", "variables"}

# Computed-only attributes are not expected in the hand-written model
def is_computed_only(attr: dict) -> bool:
    return attr.get("computed", False) and not attr.get("optional", False) and not attr.get("required", False)


def get_schema_fields(resource_schema: dict) -> set[str]:
    """Fields the user can set (non-computed-only, non-id)."""
    attrs = resource_schema["block"].get("attributes", {})
    block_types = resource_schema["block"].get("block_types", {})

    fields = set()
    for name, attr in attrs.items():
        if name == "id":
            continue
        if is_computed_only(attr):
            continue
        fields.add(name)

    for name in block_types:
        if name == "provider_config":
            continue
        fields.add(name)

    return fields


def get_laktory_fields(module_name: str) -> set[str]:
    """Import the Laktory model and return its user-visible field names."""
    try:
        mod = importlib.import_module(f"laktory.models.resources.databricks.{module_name}")
    except ImportError as e:
        return set(), str(e)

    # Find the main resource class (non-Lookup, non-mixin)
    for name in dir(mod):
        obj = getattr(mod, name)
        if (
            isinstance(obj, type)
            and hasattr(obj, "model_fields")
            and not name.endswith("Lookup")
            and not name.endswith("Grant")
            and not name.startswith("_")
            and name[0].isupper()
            and module_name.replace("_", "").lower() in name.lower()
        ):
            all_fields = set(obj.model_fields.keys())
            return all_fields - BASE_FIELDS, None

    return set(), "class not found"


def main():
    schema = json.loads(SCHEMA_PATH.read_text())
    resource_schemas = schema["provider_schemas"][PROVIDER_KEY]["resource_schemas"]

    for resource_key, module_name in TARGETS.items():
        print(f"\n{'='*60}")
        print(f"Resource: {resource_key}  (module: {module_name})")
        print("=" * 60)

        if resource_key not in resource_schemas:
            print("  [SKIP] not in schema")
            continue

        schema_fields = get_schema_fields(resource_schemas[resource_key])
        laktory_fields, err = get_laktory_fields(module_name)

        if err:
            print(f"  [ERROR] Could not load laktory model: {err}")
            print(f"  Schema fields ({len(schema_fields)}): {sorted(schema_fields)}")
            continue

        in_schema_not_laktory = schema_fields - laktory_fields
        in_laktory_not_schema = laktory_fields - schema_fields
        in_both = schema_fields & laktory_fields

        print(f"  Schema fields (user-settable):  {len(schema_fields)}")
        print(f"  Laktory fields (excl. base):    {len(laktory_fields)}")
        print(f"  In both:                        {len(in_both)}")
        print()

        if in_schema_not_laktory:
            print(f"  >> In Terraform schema but NOT in Laktory ({len(in_schema_not_laktory)}) [potential gaps]:")
            for f in sorted(in_schema_not_laktory):
                print(f"       - {f}")
        else:
            print("  >> No gaps: all schema fields are present in Laktory model")

        print()
        if in_laktory_not_schema:
            print(f"  >> In Laktory but NOT in Terraform schema ({len(in_laktory_not_schema)}) [Laktory extensions]:")
            for f in sorted(in_laktory_not_schema):
                print(f"       + {f}")
        else:
            print("  >> No extensions: Laktory model has no fields beyond the schema")


if __name__ == "__main__":
    main()
