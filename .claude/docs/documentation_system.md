# Laktory Documentation System

> **Ownership**: Claude is the designated maintainer of this documentation infrastructure.
> The user does not manually edit the griffe extension, doc stubs, or automation script.

---

## Stack Overview

| Layer | Technology | Config |
|-------|-----------|--------|
| Site generator | MkDocs 1.x (pinned `>=1.6,<2`) | `mkdocs.yml` |
| Theme | Material for MkDocs (pinned `>=9,<10`) | `mkdocs.yml` |
| API extraction | mkdocstrings[python] + griffe | `mkdocs.yml` |
| Field extraction | griffe_fieldz (include_inherited=True) | `mkdocs.yml` |
| Custom post-processing | `laktory/griffe_ext.py` | `mkdocs.yml` |
| Doc stub automation | `docs/gen_api_docs.py` | run manually |

**Build command**: `mkdocs build --strict` (must pass with 0 errors/warnings)

**Version pins rationale**: MkDocs 2.0 is coming with breaking changes (TOML config required, no migration path). Material for MkDocs enters maintenance-only at v9.7 with support ending ~November 2026. Pins in `pyproject.toml` `[dev]` block prevent accidental upgrades.

---

## Doc Stub Format

Each class gets a `.md` file under `docs/api/models/` mirroring the source directory structure.

```markdown
::: laktory.models.datasinks.TableDataSink

---

::: laktory.models.datasinks.tabledatasink.HelperClass
```

**Import path rules**:
- Use the **shortest griffe-resolvable path** — i.e., the shallowest `__init__.py` that explicitly (non-wildcard) re-exports the class
- Example: `laktory.models.datasinks.TableDataSink` (via `datasinks/__init__.py`) not `laktory.models.datasinks.tabledatasink.TableDataSink`
- `laktory/models/__init__.py` uses `from .datasinks import *` — griffe cannot follow wildcards statically, so `laktory.models.TableDataSink` does NOT work even though it's importable at runtime
- Exception: classes with name collisions across sub-packages use the full module path (e.g., `laktory.models.resources.databricks.pipeline.Pipeline` vs `laktory.models.pipeline.Pipeline`)
- Primary class appears first; helper/nested classes follow, separated by `---`

---

## Automation Script: `docs/gen_api_docs.py`

### Purpose
Detects classes in `laktory/models/` that lack a `:::` directive anywhere in `docs/api/models/`, and generates stub `.md` files for them.

### Usage
```bash
# Dry-run: report missing pages
python docs/gen_api_docs.py

# Write missing files + print nav YAML to paste into mkdocs.yml
python docs/gen_api_docs.py --write

# Print nav YAML for ALL model files (full regeneration)
python docs/gen_api_docs.py --nav-all
```

### How it works

1. **`_collect_documented_classes()`** — scans all `docs/api/models/*.md` for `:::` directives; registers `{ClassName: md_file}`. Detection is class-name-based, not file-path-based, to avoid false positives when a class is documented under an abbreviated filename.

2. **`_build_import_map()`** — scans `laktory/models/**/` `__init__.py` files **shallowest-first** (ascending path depth), skipping wildcard imports (`from .X import *`). Produces `{ClassName: shortest_griffe_resolvable_path}`. This is AST-based (not runtime introspection) because griffe resolves paths statically.

3. **`_primary_class(classes, stem)`** — heuristic: match the file stem (lowercased, underscores removed) against class names. Exact match first, then substring, then fallback to first class in file.

4. **`_md_content(classes, primary, py_file, import_map)`** — generates stub content with primary class first, helpers alphabetically after.

5. **`_build_nav(records)`** — generates YAML nav entries grouped by top-level directory. Paste output into `mkdocs.yml` under `API Reference > Models`.

### Skip lists

```python
SKIP_CLASSES = {"BaseChild", "ModelMetaclass"}          # never get own page
SKIP_FILES   = {"basechild.py"}                         # files to ignore entirely
SKIP_DIRS    = {"azurenative"}                          # dirs without __init__.py (griffe can't resolve)
```

**`azurenative/` is skipped** because it has no `__init__.py` and no public exports — griffe cannot statically traverse to its classes.

### After running `--write`
1. Add the printed nav YAML entries to `mkdocs.yml` in the appropriate section
2. Run `mkdocs build --strict` to verify

---

## Custom Griffe Extension: `laktory/griffe_ext.py`

Registered in `mkdocs.yml` after `griffe_fieldz`:
```yaml
extensions:
  - griffe_fieldz:
      include_inherited: True
  - laktory.griffe_ext:
```

The extension hooks `on_class` (runs after griffe has fully built the class object tree). It does three things:

### 1. Hide base class fields and methods (`__doc_hide_base__`)

Infrastructure base classes marked with `__doc_hide_base__ = True` have ALL their public fields and methods suppressed in child class documentation:

```python
# Currently marked:
class BaseModel(...):
    __doc_hide_base__ = True   # laktory/models/basemodel.py

class BaseChild(BaseModel, ...):
    __doc_hide_base__ = True   # laktory/models/basechild.py

class PipelineChild(BaseChild):
    __doc_hide_base__ = True   # laktory/models/pipelinechild.py
```

**Mechanism**: griffe's `inherited_members` property returns members NOT already in `cls.members`. By adding no-docstring `Attribute` stubs to `cls.members` for every public member of the hidden base, those inherited members are excluded from `inherited_members`. With `show_if_no_docstring: false` in `mkdocs.yml`, the stubs themselves are also hidden.

**Fragility note**: This depends on griffe's `inherited_members` checking `if name not in self.members`. If griffe changes that implementation, the hiding will break silently.

### 2. Hide specific fields per class (`__hidden_doc_fields__`)

Individual child classes can hide specific inherited fields without hiding the entire base:

```python
class MyModel(SomeParent):
    __hidden_doc_fields__: ClassVar[set[str]] = {"field_to_hide", "another_field"}
```

This accumulates across the MRO — each class in the chain can add its own set.

### 3. Sort fields alphabetically

All visible fields (own + inherited, after hiding) are sorted alphabetically within their parameter/attribute section. This gives consistent ordering across all model documentation pages.

### 4. Fix `VariableType` hyperlinks (issue #444)

**Root cause**: `laktory.typing.VariableType` is a custom `str` subclass injected at runtime into all Pydantic model field types by `ModelMetaclass` (in `basemodel.py`). Source files declare `path: str` but at runtime it becomes `Union[str, VariableType]`. griffe_fieldz reads runtime field types and correctly identifies the union, but its `display_as_type()` function strips module prefixes, producing the string `"bool | VariableType"`. When parsed into a griffe `ExprName('VariableType')`, there is no `parent` context pointing to `laktory.typing`, so autorefs can't resolve the link.

**Fix**: In `on_class`, after all field sections are populated:
1. Get the `laktory.typing` griffe module via `cls.package["typing"]`
2. Create `ExprName("VariableType", parent=typing_module)` — this gives `canonical_path = "laktory.typing.VariableType"`
3. Recursively walk all parameter annotation expression trees (`_replace_expr_name`) replacing bare `ExprName("VariableType")` with the qualified version

`_replace_expr_name` handles `ExprBinOp` (e.g., `bool | VariableType`), `ExprSubscript` (e.g., `list[VariableType]`), and base `ExprName` nodes.

**If `VariableType` is renamed or moved**: update the `ExprName("VariableType", ...)` call in `on_class` and the lookup `cls.package["typing"]`.

**If other types from `laktory.typing` need the same treatment**: add additional `_replace_expr_name` calls with the same pattern.

---

## Model Class Conventions for Documentation

### `__doc_hide_base__ = True`
Add to a base class when ALL its fields and methods should be invisible in child class docs. The base class gets its own documentation page (its fields are shown there), but children don't repeat them.

### `__hidden_doc_fields__: ClassVar[set[str]]`
Add to a child class to hide specific inherited fields without hiding the entire parent. Useful when a parent's most fields are relevant to children but a few are implementation details.

### Docstrings
mkdocstrings uses the **NumPy docstring convention** (`pydocstyle.convention = "numpy"` in `pyproject.toml`). Field descriptions come from `Field(description="...")` in Pydantic, extracted by griffe_fieldz. Class-level narrative goes in the class docstring.

---

## Known Issues / Watch Points

| Issue | Status | Notes |
|-------|--------|-------|
| `__doc_hide_base__` stub mechanism is fragile | Open | Depends on griffe internals; acceptable for now |
| `laktory/models/__init__.py` uses `from .X import *` | By design | Prevents shortest `laktory.models.X` paths; would need explicit imports to fix |
| `azurenative/` has no `__init__.py` | Open | Classes there can't be documented until a `__init__.py` is added |
| MkDocs 2.0 migration | Deferred | Plan to migrate before ~Nov 2026; evaluate griffe_pydantic first |
| Two `Pipeline` classes (Laktory vs DLT resource) | Known | Both named `Pipeline`; `resources/databricks/pipeline.md` must keep full module path |

---

## Verification Checklist

After any documentation change:
```bash
mkdocs build --strict          # 0 errors, 0 warnings
python docs/gen_api_docs.py    # 0 missing
```
