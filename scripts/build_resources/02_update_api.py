"""
Generates MkDocs API stub files under docs/api/models/resources/databricks/
from the built *_base.py modules and their override files.

Usage:
    python scripts/build_resources/02_update_api.py [resource_key ...]

If no resource keys are given, all DEFAULT_TARGETS are processed.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _config import DEFAULT_TARGETS  # noqa: E402
from _config import RESOURCE_NAME_OVERRIDES  # noqa: E402
from _config import base_file_stem  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_base_override_map() -> dict[str, tuple[Path, str]]:
    """Return a map from base stem -> (override file path, file text).

    Reads each non-base .py file once and records which *_base module it
    imports from, so callers avoid repeated directory scans.
    """
    pattern = re.compile(
        r"from\s+(?:\.|laktory\.models\.resources\.databricks\.)(\w+)_base\s+import"
    )
    result: dict[str, tuple[Path, str]] = {}
    for py_file in sorted(DATABRICKS_DIR.glob("*.py")):
        if "_base" in py_file.name or py_file.name == "__init__.py":
            continue
        text = py_file.read_text()
        m = pattern.search(text)
        if m:
            result[m.group(1)] = (py_file, text)
    return result


def extract_public_class(text: str) -> str | None:
    """Return the name of the main public class (inherits from *Base, not a Lookup).

    Requires the parent class to END with 'Base' (e.g. CatalogBase), which
    correctly excludes classes that inherit from BaseModel or BaseResource.
    """
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


def extract_lookup_class(text: str) -> str | None:
    """Return the name of the *Lookup class if one is defined, else None."""
    m = re.search(r"^class\s+(\w+Lookup)\s*\(", text, re.MULTILINE)
    return m.group(1) if m else None


def extract_override_helper_classes(
    text: str, public_class: str, already_known: set[str]
) -> list[str]:
    """Return names of hand-authored helper classes defined in the override file.

    Excludes the public class, the Lookup class, and anything already collected
    from the *_base __all__ to avoid duplicates.
    """
    return [
        m.group(1)
        for m in re.finditer(r"^class\s+(\w+)\s*\(", text, re.MULTILINE)
        if m.group(1) not in already_known
        and m.group(1) != public_class
        and not m.group(1).endswith("Lookup")
    ]


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

    # Build the base→override map once so we don't re-scan for every resource
    override_map = build_base_override_map()

    written: list[Path] = []

    for resource_key in targets:
        naming_key = RESOURCE_NAME_OVERRIDES.get(resource_key, resource_key)
        bstem = base_file_stem(naming_key)

        base_file = DATABRICKS_DIR / f"{bstem}_base.py"
        if not base_file.exists():
            print(f"[SKIP] {resource_key} — {base_file.name} not found")
            continue

        if bstem not in override_map:
            print(f"[SKIP] {resource_key} — no override file found for {bstem}_base")
            continue
        override_file, text = override_map[bstem]

        public_class = extract_public_class(text)
        if public_class is None:
            print(
                f"[SKIP] {resource_key} — could not find public class in {override_file.name}"
            )
            continue

        nested_classes = read_base_all(base_file)
        lookup_class = extract_lookup_class(text)
        if lookup_class:
            nested_classes.append(lookup_class)
        override_helpers = extract_override_helper_classes(
            text, public_class, set(nested_classes)
        )
        nested_classes.extend(override_helpers)

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
