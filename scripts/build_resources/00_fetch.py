"""
Refreshes databricks_schema.json and databricks_descriptions.json for a specific
Databricks Terraform provider version.

Steps:
  1. Creates a temporary Terraform workspace with the pinned provider version.
  2. Runs `terraform init` + `terraform providers schema -json` to extract the schema.
  3. Fetches Go model files from GitHub at the matching version tag and parses
     field descriptions.

Usage:
    python scripts/build_resources/00_fetch.py 1.81.1
    python scripts/build_resources/00_fetch.py 1.81.1 --schema-only
    python scripts/build_resources/00_fetch.py 1.81.1 --descriptions-only

Outputs (written to the same directory as this script):
    databricks_schema.json
    databricks_descriptions.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import urllib.request
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent
SCHEMA_PATH = SCRIPTS_DIR / "databricks_schema.json"
DESCRIPTIONS_PATH = SCRIPTS_DIR / "databricks_descriptions.json"

GITHUB_RAW = (
    "https://raw.githubusercontent.com/databricks/terraform-provider-databricks"
)

# Mirrors the mapping in 00_fetch.py
RESOURCE_STRUCTS: dict[str, dict] = {
    "databricks_catalog": {
        "service": "catalog_tf",
        "primary": "CatalogInfo",
        "fallbacks": ["CreateCatalog", "UpdateCatalog"],
    },
    "databricks_metastore": {
        "service": "catalog_tf",
        "primary": "MetastoreInfo",
        "fallbacks": ["CreateMetastore", "UpdateMetastore"],
    },
    "databricks_schema": {
        "service": "catalog_tf",
        "primary": "SchemaInfo",
        "fallbacks": ["CreateSchema", "UpdateSchema"],
    },
    "databricks_volume": {
        "service": "catalog_tf",
        "primary": "VolumeInfo",
        "fallbacks": ["CreateVolumeRequestContent", "UpdateVolumeRequestContent"],
    },
    "databricks_cluster": {
        "service": "compute_tf",
        "primary": "ClusterDetails",
        "fallbacks": ["ClusterSpec", "CreateCluster", "EditCluster"],
    },
}

MAIN_TF_TEMPLATE = """\
terraform {{
  required_providers {{
    databricks = {{
      source  = "databricks/databricks"
      version = "{version}"
    }}
  }}
}}
"""


# ---------------------------------------------------------------------------
# Schema generation
# ---------------------------------------------------------------------------


def generate_schema(version: str) -> None:
    """Download the provider and extract its schema JSON."""
    print(f"\n── Schema (provider v{version}) ──────────────────────────────")
    with tempfile.TemporaryDirectory(prefix="laktory-tf-") as tmpdir:
        tf_dir = Path(tmpdir)
        (tf_dir / "main.tf").write_text(MAIN_TF_TEMPLATE.format(version=version))

        print("  terraform init ...")
        subprocess.run(
            ["terraform", "init", "-no-color"],
            cwd=tf_dir,
            check=True,
        )

        print("  terraform providers schema -json ...")
        result = subprocess.run(
            ["terraform", "providers", "schema", "-json", "-no-color"],
            cwd=tf_dir,
            check=True,
            stdout=subprocess.PIPE,  # captured for JSON parsing
            text=True,
            # stderr intentionally not captured so errors print to terminal
        )

    # Validate and pretty-print
    schema = json.loads(result.stdout)
    SCHEMA_PATH.write_text(json.dumps(schema, indent=2, sort_keys=True))
    resource_count = len(
        schema.get("provider_schemas", {})
        .get("registry.terraform.io/databricks/databricks", {})
        .get("resource_schemas", {})
    )
    print(f"  Saved {resource_count} resource schemas → {SCHEMA_PATH.name}")


# ---------------------------------------------------------------------------
# Description fetching (duplicated/inlined from 00_fetch.py
# so this script is self-contained)
# ---------------------------------------------------------------------------


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode()


def parse_all_structs(go_source: str) -> dict[str, dict[str, str]]:
    import re

    structs: dict[str, dict[str, str]] = {}
    current_struct: str | None = None
    lines = go_source.splitlines()

    for i, line in enumerate(lines):
        struct_m = re.match(r"^type\s+(\w+)\s+struct\s*\{", line)
        if struct_m:
            current_struct = struct_m.group(1)
            structs[current_struct] = {}
            continue

        if current_struct is None:
            continue

        if line == "}":
            current_struct = None
            continue

        tfsdk_m = re.search(r'`tfsdk:"([^"]+)"', line)
        if not tfsdk_m:
            continue
        attr_name = tfsdk_m.group(1)
        if attr_name == "-":
            continue

        comment_lines: list[str] = []
        j = i - 1
        while j >= 0 and lines[j].strip().startswith("//"):
            comment_lines.insert(0, lines[j].strip())
            j -= 1

        first_para: list[str] = []
        for cl in comment_lines:
            if cl.strip() == "//":
                break
            text = cl.lstrip("/ ").strip()
            if re.match(r"\[.*?\]:\s+https?://", text):
                continue
            first_para.append(text)

        desc = " ".join(first_para).strip()
        if desc:
            structs[current_struct][attr_name] = desc

    return structs


def build_resource_descriptions(
    all_structs: dict[str, dict[str, str]],
    primary: str,
    fallbacks: list[str],
) -> dict[str, str]:
    result = dict(all_structs.get(primary, {}))
    for fb in fallbacks:
        for attr, desc in all_structs.get(fb, {}).items():
            if attr not in result:
                result[attr] = desc
    return result


def generate_descriptions(version: str) -> None:
    """Fetch Go model files from GitHub at the given version tag and parse descriptions."""
    tag = f"v{version}"
    print(f"\n── Descriptions (tag {tag}) ──────────────────────────────────")

    service_to_resources: dict[str, list[str]] = {}
    for resource_key, cfg in RESOURCE_STRUCTS.items():
        service_to_resources.setdefault(cfg["service"], []).append(resource_key)

    service_structs: dict[str, dict[str, dict[str, str]]] = {}
    for service_dir in service_to_resources:
        url = f"{GITHUB_RAW}/{tag}/internal/service/{service_dir}/model.go"
        print(f"  Fetching {service_dir}/model.go ...")
        try:
            source = fetch_text(url)
        except Exception as exc:
            print(f"  [WARN] Could not fetch {url}: {exc}")
            service_structs[service_dir] = {}
            continue
        parsed = parse_all_structs(source)
        service_structs[service_dir] = parsed
        print(f"    {len(parsed)} structs parsed")

    cache: dict[str, dict[str, str]] = {}
    for resource_key, cfg in RESOURCE_STRUCTS.items():
        all_structs = service_structs.get(cfg["service"], {})
        descs = build_resource_descriptions(
            all_structs, cfg["primary"], cfg["fallbacks"]
        )
        cache[resource_key] = descs
        print(f"    {resource_key}: {len(descs)} field descriptions")

    DESCRIPTIONS_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True))
    print(f"  Saved → {DESCRIPTIONS_PATH.name}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh Databricks provider schema and field descriptions."
    )
    # parser.add_argument("version", help="Provider version, e.g. 1.81.1")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--schema-only",
        action="store_true",
        help="Only regenerate databricks_schema.json",
    )
    group.add_argument(
        "--descriptions-only",
        action="store_true",
        help="Only regenerate databricks_descriptions.json",
    )
    args = parser.parse_args()

    version = "1.113.0"

    if not args.descriptions_only:
        generate_schema(version)

    if not args.schema_only:
        generate_descriptions(version)

    print(
        "\nDone. Run `python scripts/build_resources/01_build.py` to regenerate base classes."
    )


if __name__ == "__main__":
    main()
