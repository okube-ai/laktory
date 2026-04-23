"""
Generates MkDocs API stub files under docs/api/models/resources/databricks/
from the built *_base.py modules and their override files.

Usage:
    python scripts/build_resources/02_update_api.py [resource_key ...]

If no resource keys are given, all DEFAULT_TARGETS are processed.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPTS_DIR = Path(__file__).parent
DATABRICKS_DIR = (
    Path(__file__).parent.parent.parent
    / "laktory"
    / "models"
    / "resources"
    / "databricks"
)
DOCS_DIR = (
    Path(__file__).parent.parent.parent
    / "docs"
    / "api"
    / "models"
    / "resources"
    / "databricks"
)
LAKTORY_PKG = "laktory.models.resources.databricks"

# Load shared constants from 01_build.py without executing its main()
_spec = importlib.util.spec_from_file_location("_build01", SCRIPTS_DIR / "01_build.py")
_build01 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_build01)

DEFAULT_TARGETS: list[str] = _build01.DEFAULT_TARGETS
RESOURCE_NAME_OVERRIDES: dict[str, str] = _build01.RESOURCE_NAME_OVERRIDES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def base_file_stem(naming_key: str) -> str:
    """Return the stem of the generated *_base.py file for a naming key.

    "databricks_sql_endpoint" -> "sqlendpoint"
    """
    return naming_key.removeprefix("databricks_").replace("_", "")


def find_override_file(bstem: str) -> Path | None:
    """Return the non-base .py file that imports from {bstem}_base, or None."""
    pattern = re.compile(
        rf"from\s+(?:\.|laktory\.models\.resources\.databricks\.){re.escape(bstem)}_base\s+import"
    )
    for py_file in sorted(DATABRICKS_DIR.glob("*.py")):
        if "_base" in py_file.name or py_file.name == "__init__.py":
            continue
        if pattern.search(py_file.read_text()):
            return py_file
    return None


def extract_public_class(override_file: Path) -> str | None:
    """Return the name of the main public class (inherits from *Base, not a Lookup).

    Requires the parent class to END with 'Base' (e.g. CatalogBase), which
    correctly excludes classes that inherit from BaseModel or BaseResource.
    """
    text = override_file.read_text()
    # \w+Base(?=\s*[,)]) ensures 'Base' ends the parent class name, not e.g. BaseModel
    for m in re.finditer(
        r"^class\s+(\w+)\s*\([^)]*\w+Base(?=\s*[,)])[^)]*\)",
        text,
        re.MULTILINE,
    ):
        name = m.group(1)
        if not name.endswith("Lookup"):
            return name
    return None


def extract_lookup_class(override_file: Path) -> str | None:
    """Return the name of the *Lookup class if one is defined, else None."""
    text = override_file.read_text()
    m = re.search(r"^class\s+(\w+Lookup)\s*\(", text, re.MULTILINE)
    return m.group(1) if m else None


def read_base_all(base_file: Path) -> list[str]:
    """Parse __all__ from a *_base.py file, excluding the *Base entry."""
    text = base_file.read_text()
    m = re.search(r"__all__\s*=\s*(\[.*?\])", text, re.DOTALL)
    if not m:
        return []
    names: list[str] = ast.literal_eval(m.group(1))
    return [n for n in names if not n.endswith("Base")]


def emit_doc(public_class: str, module_stem: str, nested_classes: list[str]) -> str:
    """Generate full .md content for a resource doc stub."""
    entries = [f"::: {LAKTORY_PKG}.{public_class}"]
    for cls in sorted(nested_classes):
        entries.append(f"::: {LAKTORY_PKG}.{module_stem}.{cls}")
    return (
        "<!-- GENERATED FILE — DO NOT EDIT -->\n" + "\n\n---\n\n".join(entries) + "\n"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(targets: list[str] | None = None) -> None:
    if targets is None:
        targets = list(sys.argv[1:]) or DEFAULT_TARGETS

    written: list[Path] = []

    for resource_key in targets:
        naming_key = RESOURCE_NAME_OVERRIDES.get(resource_key, resource_key)
        bstem = base_file_stem(naming_key)

        base_file = DATABRICKS_DIR / f"{bstem}_base.py"
        if not base_file.exists():
            print(f"[SKIP] {resource_key} — {base_file.name} not found")
            continue

        override_file = find_override_file(bstem)
        if override_file is None:
            print(f"[SKIP] {resource_key} — no override file found for {bstem}_base")
            continue

        public_class = extract_public_class(override_file)
        if public_class is None:
            print(
                f"[SKIP] {resource_key} — could not find public class in {override_file.name}"
            )
            continue

        nested_classes = read_base_all(base_file)
        lookup_class = extract_lookup_class(override_file)
        if lookup_class:
            nested_classes.append(lookup_class)

        content = emit_doc(public_class, override_file.stem, nested_classes)
        doc_path = DOCS_DIR / f"{override_file.stem}.md"
        doc_path.write_text(content)
        written.append(doc_path)
        print(
            f"[OK]   {resource_key} -> {doc_path.relative_to(Path(__file__).parent.parent.parent)}"
        )

    if written:
        print(f"[OK]   {len(written)} doc file(s) written")


if __name__ == "__main__":
    main()
