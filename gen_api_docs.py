#!/usr/bin/env python
"""
Generate missing API reference documentation pages for laktory/models/.

Scans laktory/models/ for Python files, cross-references against existing
docs/api/models/ pages (by class name, not by filename), and either reports
or writes the missing .md stubs.

Usage
-----
    # Report what is missing (dry-run)
    python scripts/gen_api_docs.py

    # Write missing .md files and print nav YAML to stdout
    python scripts/gen_api_docs.py --write

    # Print nav YAML for ALL model files (to regenerate mkdocs.yml entries)
    python scripts/gen_api_docs.py --nav-all
"""

from __future__ import annotations

import argparse
import ast
import re
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
MODELS_SRC = ROOT / "laktory" / "models"
DOCS_MODELS = ROOT / "docs" / "api" / "models"

# ---------------------------------------------------------------------------
# Classes that should never get their own doc page (infrastructure internals)
# ---------------------------------------------------------------------------
SKIP_CLASSES = {
    "BaseChild",
    "ModelMetaclass",
}

# Python source files whose classes are intentionally undocumented
SKIP_FILES = {
    "basechild.py",
}

# Directories (relative to MODELS_SRC) whose files are intentionally undocumented
SKIP_DIRS = {
    "azurenative",  # no __init__.py; not publicly exported; griffe cannot resolve
}


# ---------------------------------------------------------------------------
# Step 1 – Collect every class already referenced via a ::: directive
# ---------------------------------------------------------------------------


def _collect_documented_classes() -> dict[str, Path]:
    """Return {short_class_name: md_file} for every ::: directive found."""
    documented: dict[str, Path] = {}
    for md in DOCS_MODELS.rglob("*.md"):
        for line in md.read_text().splitlines():
            m = re.match(r"^:::\s+([\w.]+)", line)
            if m:
                full = m.group(1)
                short = full.rsplit(".", 1)[-1]  # just the class name
                documented[short] = md
    return documented


# ---------------------------------------------------------------------------
# Step 2 – Build canonical import path map from __init__.py files
# ---------------------------------------------------------------------------


def _build_import_map() -> dict[str, str]:
    """Return {ClassName: shortest_griffe-resolvable_dotted_path}.

    Scans ``__init__.py`` files shallowest-first so that a sub-package that
    explicitly re-exports a class (e.g. ``models.datasinks.TableDataSink``)
    wins over the deeper module path.  Wildcard imports (``import *``) are
    intentionally skipped because griffe cannot follow them statically.
    """
    import_map: dict[str, str] = {}

    for init in sorted(MODELS_SRC.rglob("__init__.py"), key=lambda p: len(p.parts)):
        rel = init.parent.relative_to(ROOT / "laktory")
        pkg = "laktory." + str(rel).replace("/", ".")
        try:
            tree = ast.parse(init.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "*":
                        continue  # wildcards are not statically resolvable by griffe
                    name = alias.asname or alias.name
                    if name and name[0].isupper() and name not in import_map:
                        import_map[name] = f"{pkg}.{name}"

    return import_map


# ---------------------------------------------------------------------------
# Step 3 – Discover classes in each Python source file
# ---------------------------------------------------------------------------


def _classes_in_file(py_file: Path) -> list[str]:
    try:
        tree = ast.parse(py_file.read_text())
    except SyntaxError:
        return []
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
    ]


def _primary_class(classes: list[str], stem: str) -> str | None:
    """The class whose name most closely matches the file stem."""
    stem_lower = stem.lower().replace("_", "")
    for cls in classes:
        if cls.lower() == stem_lower:
            return cls
    for cls in classes:
        if stem_lower in cls.lower():
            return cls
    return classes[0] if classes else None


# ---------------------------------------------------------------------------
# Step 4 – Canonical import path for a class
# ---------------------------------------------------------------------------


def _import_path(class_name: str, py_file: Path, import_map: dict[str, str]) -> str:
    if class_name in import_map:
        return import_map[class_name]
    rel = py_file.relative_to(ROOT / "laktory")
    module = str(rel.with_suffix("")).replace("/", ".")
    return f"laktory.{module}.{class_name}"


# ---------------------------------------------------------------------------
# Step 5 – Build records for every model source file
# ---------------------------------------------------------------------------


def _discover_model_files(
    documented: dict[str, Path],
    import_map: dict[str, str],
) -> list[dict]:
    records = []
    for py_file in sorted(MODELS_SRC.rglob("*.py")):
        if py_file.name.startswith("_"):
            continue
        if py_file.name in SKIP_FILES:
            continue
        rel_parts = py_file.relative_to(MODELS_SRC).parts
        if any(part in SKIP_DIRS for part in rel_parts):
            continue
        if "__pycache__" in py_file.parts:
            continue

        rel = py_file.relative_to(MODELS_SRC)
        classes = [c for c in _classes_in_file(py_file) if c not in SKIP_CLASSES]
        if not classes:
            continue

        primary = _primary_class(classes, py_file.stem)

        # A file is "documented" if its primary class appears in any ::: directive
        is_documented = primary in documented if primary else False

        # Expected doc path mirrors the source directory structure
        expected_doc = DOCS_MODELS / rel.with_suffix(".md")
        # If already documented in a different file, record that path instead
        actual_doc = (
            documented.get(primary, expected_doc) if is_documented else expected_doc
        )

        records.append(
            dict(
                py_file=py_file,
                rel=rel,
                classes=classes,
                primary=primary,
                is_documented=is_documented,
                doc_path=actual_doc,
                expected_doc=expected_doc,
            )
        )

    return records


# ---------------------------------------------------------------------------
# Step 6 – Generate .md content
# ---------------------------------------------------------------------------


def _md_content(
    classes: list[str], primary: str | None, py_file: Path, import_map: dict[str, str]
) -> str:
    ordered = sorted(classes, key=lambda c: (0 if c == primary else 1, c))
    blocks = [f"::: {_import_path(cls, py_file, import_map)}" for cls in ordered]
    return "\n\n---\n\n".join(blocks) + "\n"


# ---------------------------------------------------------------------------
# Step 7 – Nav YAML
# ---------------------------------------------------------------------------


def _nav_label(cls: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", cls)


def _build_nav(records: list[dict]) -> str:
    groups: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        top = rec["rel"].parts[0] if len(rec["rel"].parts) > 1 else ""
        groups[top].append(rec)

    lines = []
    for top in sorted(groups):
        if top:
            lines.append(f"        - {top.capitalize()}:")
        for rec in sorted(groups[top], key=lambda r: r["rel"]):
            label = (
                _nav_label(rec["primary"]) if rec["primary"] else rec["py_file"].stem
            )
            rel_doc = rec["expected_doc"].relative_to(ROOT / "docs")
            lines.append(f"          - {label}: {rel_doc}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--write", action="store_true", help="Write missing .md files")
    parser.add_argument(
        "--nav-all",
        action="store_true",
        help="Print nav YAML for ALL model files (not just missing ones)",
    )
    args = parser.parse_args()

    import_map = _build_import_map()
    documented = _collect_documented_classes()
    records = _discover_model_files(documented, import_map)

    missing = [r for r in records if not r["is_documented"]]
    existing = [r for r in records if r["is_documented"]]

    # Report -----------------------------------------------------------------
    print(f"Model source files    : {len(records)}")
    print(f"Documented            : {len(existing)}")
    print(f"Missing documentation : {len(missing)}")
    print()

    if missing:
        print("MISSING documentation:")
        for rec in missing:
            extra = (
                f"  [{', '.join(c for c in rec['classes'] if c != rec['primary'])}]"
                if len(rec["classes"]) > 1
                else ""
            )
            print(
                f"  {rec['expected_doc'].relative_to(ROOT)}  ({rec['primary']}{extra})"
            )
        print()

    # Write ------------------------------------------------------------------
    if args.write:
        written = 0
        for rec in missing:
            content = _md_content(
                rec["classes"], rec["primary"], rec["py_file"], import_map
            )
            rec["expected_doc"].parent.mkdir(parents=True, exist_ok=True)
            rec["expected_doc"].write_text(content)
            written += 1
            print(f"  wrote  {rec['expected_doc'].relative_to(ROOT)}")
        if written:
            print(f"\nWrote {written} file(s).")
            print()

    # Nav YAML ---------------------------------------------------------------
    if args.write or args.nav_all:
        nav_records = records if args.nav_all else missing
        if nav_records:
            print("Nav YAML (paste into mkdocs.yml under API Reference > Models):")
            print("-" * 64)
            print(_build_nav(nav_records))
            print("-" * 64)
    elif missing:
        print("Run with --write to create the missing files and get the nav YAML.")


if __name__ == "__main__":
    main()
