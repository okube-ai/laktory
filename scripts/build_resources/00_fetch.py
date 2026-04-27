"""
Refreshes databricks_schema.json and databricks_descriptions.json for a specific
Databricks Terraform provider version.

Steps:
  1. Creates a temporary Terraform workspace with the pinned provider version.
  2. Runs `terraform init` + `terraform providers schema -json` to extract the schema.
  3. Fetches Go model files from GitHub at the matching version tag and parses
     field descriptions.

Usage:
    python scripts/build_resources/00_fetch.py [version]
    python scripts/build_resources/00_fetch.py 1.81.1
    python scripts/build_resources/00_fetch.py 1.81.1 --schema-only
    python scripts/build_resources/00_fetch.py 1.81.1 --descriptions-only

    If version is omitted, defaults to 1.113.0.

Outputs (written to the same directory as this script):
    databricks_schema.json
    databricks_descriptions.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent
SCHEMA_PATH = SCRIPTS_DIR / "databricks_schema.json"
DESCRIPTIONS_PATH = SCRIPTS_DIR / "databricks_descriptions.json"

GITHUB_RAW = (
    "https://raw.githubusercontent.com/databricks/terraform-provider-databricks"
)

DEFAULT_VERSION = "1.113.0"

# Resources whose descriptions come from Go model struct comments.
# Format: resource_key → {service dir, primary struct, fallback structs}
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

# Resources whose descriptions are fetched from provider docs markdown files.
# Each entry maps the terraform resource name to its docs filename stem
# (usually the resource name without the "databricks_" prefix).
MARKDOWN_TARGETS: list[str] = [
    "databricks_access_control_rule_set",
    "databricks_alert",
    "databricks_app",
    "databricks_cluster_policy",
    "databricks_dashboard",
    "databricks_dbfs_file",
    "databricks_directory",
    "databricks_external_location",
    "databricks_group",
    "databricks_group_member",
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
    "databricks_quality_monitor",
    "databricks_query",
    "databricks_recipient",
    "databricks_repo",
    "databricks_service_principal",
    "databricks_service_principal_role",
    "databricks_sql_endpoint",
    "databricks_storage_credential",
    "databricks_user",
    "databricks_user_role",
    "databricks_vector_search_endpoint",
    "databricks_vector_search_index",
    "databricks_workspace_file",
    # Phase 2 — resources with grants/child resources
    "databricks_grant",
    "databricks_grants",
    "databricks_permissions",
    "databricks_registered_model",
    "databricks_secret",
    "databricks_secret_acl",
    "databricks_secret_scope",
    "databricks_share",
    "databricks_sql_table",
    "databricks_workspace_binding",
    # Phase 3 — complex resources
    "databricks_job",
    "databricks_pipeline",
    # Other commonly used resources
    "databricks_entitlements",
    "databricks_git_credential",
    "databricks_global_init_script",
    "databricks_instance_pool",
    "databricks_ip_access_list",
    "databricks_library",
    "databricks_model_serving",
    "databricks_online_table",
    "databricks_token",
]

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
# Description fetching
# ---------------------------------------------------------------------------


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode()


def parse_all_structs(go_source: str) -> dict[str, dict[str, str]]:
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


def parse_md_descriptions(md_source: str) -> dict[str, str]:
    """
    Parse field descriptions from a Terraform provider docs markdown file.

    Sections "Argument reference" and "Attribute reference" are scanned for
    bullet lines of the form:
        * `field_name` - [(Required|Optional)] Description text.
    Nested bullets (indented) are included using the parent heading as context
    but are stored under their own field name.
    """
    descriptions: dict[str, str] = {}
    in_ref_section = False
    pending_field: str | None = None
    pending_lines: list[str] = []

    def flush():
        nonlocal pending_field, pending_lines
        if pending_field and pending_lines:
            desc = " ".join(pending_lines).strip()
            # Strip requirement markers like "(Required)" / "(Optional)"
            desc = re.sub(r"^\((?:Required|Optional)[^)]*\)\s*", "", desc)
            desc = desc.strip(" .")
            if desc:
                descriptions[pending_field] = desc
        pending_field = None
        pending_lines = []

    for line in md_source.splitlines():
        # Detect section headers
        if re.match(r"^##\s+", line):
            in_ref_section = bool(re.search(r"argument|attribute", line, re.IGNORECASE))
            flush()
            continue

        if not in_ref_section:
            continue

        # Match a bullet: * `field_name` - description
        m = re.match(r"^\s*\*\s+`([^`]+)`\s+-\s+(.*)", line)
        if m:
            flush()
            pending_field = m.group(1)
            pending_lines = [m.group(2).strip()]
            continue

        # Continuation line for current field (non-empty, non-header)
        if pending_field and line.strip() and not line.startswith("#"):
            pending_lines.append(line.strip())
            continue

        # Blank line ends the current field description
        if not line.strip():
            flush()

    flush()
    return descriptions


def fetch_md_descriptions(resource_key: str, tag: str) -> dict[str, str]:
    """Fetch field descriptions from the provider docs markdown file."""
    stem = resource_key.removeprefix("databricks_")
    url = f"{GITHUB_RAW}/{tag}/docs/resources/{stem}.md"
    try:
        source = fetch_text(url)
        descriptions = parse_md_descriptions(source)
        print(f"    {resource_key}: {len(descriptions)} field descriptions (markdown)")
        return descriptions
    except Exception as exc:
        print(f"  [WARN] Could not fetch docs for {resource_key}: {exc}")
        return {}


def generate_descriptions(version: str) -> None:
    """Fetch descriptions from Go model files and provider docs markdown."""
    tag = f"v{version}"
    print(f"\n── Descriptions (tag {tag}) ──────────────────────────────────")

    # --- Go struct approach (high quality, original 5 resources) ---
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
        print(f"    {resource_key}: {len(descs)} field descriptions (go structs)")

    # --- Markdown docs approach — fetch all remaining resources in parallel ---
    remaining = [rk for rk in MARKDOWN_TARGETS if rk not in cache]
    print(f"\n  Fetching {len(remaining)} markdown description files in parallel ...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_md_descriptions, rk, tag): rk for rk in remaining
        }
        for future in as_completed(futures):
            rk = futures[future]
            cache[rk] = future.result()

    DESCRIPTIONS_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True))
    print(f"  Saved → {DESCRIPTIONS_PATH.name}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh Databricks provider schema and field descriptions."
    )
    parser.add_argument(
        "version",
        nargs="?",
        default=DEFAULT_VERSION,
        help=f"Provider version, e.g. 1.81.1 (default: {DEFAULT_VERSION})",
    )
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

    if not args.descriptions_only:
        generate_schema(args.version)

    if not args.schema_only:
        generate_descriptions(args.version)

    print(
        "\nDone. Run `python scripts/build_resources/01_build.py` to regenerate base classes."
    )


if __name__ == "__main__":
    main()
