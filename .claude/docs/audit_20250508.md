# Laktory Codebase Audit — Findings

_Audited May 2026. Three parallel agents reviewed the full codebase: core models, infrastructure/resources, and utilities/YAML/CLI/tests. All findings verified against source._

---

## 1. Critical / Immediate

Issues that should be fixed before the next release — production bugs, footguns, or deprecated calls that are already incorrect.

| # | File | Issue |
|---|------|-------|
| ~~C1~~ | ~~`laktory/models/datasources/tabledatasource.py:137`~~ | **Done** — replaced `print()` with `logger.debug()` |
| ~~C2~~ | ~~`laktory/models/grants/tablegrant.py:23`~~ | **Done** — module-level test instance removed |
| ~~C3~~ | ~~`laktory/cli/_common.py:77`~~ | **Done** — `logger.warn()` replaced with `logger.warning()` |
| ~~C4~~ | ~~`laktory/cli/_common.py:135,148`~~ | **Done** — `print()` statements routed through logger |
| ~~C5~~ | ~~`laktory/version.py:52`~~ | **Done** — bare `except:` narrowed to `ModuleNotFoundError` |
| C6 | `laktory/models/datasinks/mergecdcoptions.py:512` | `except Exception` too broad; catches `AttributeError`, `ImportError`, etc. silently — **won't fix**: different Spark versions (including Spark Connect) raise different exception types for a missing table; narrowing risks swallowing a legitimate "table not found" on an untested version |

---

## 2. Performance

Measurable or clearly avoidable overhead on hot paths.

| # | File | Issue |
|---|------|-------|
| ~~P1~~ | ~~`laktory/models/basemodel.py:445,450,555`~~ | **Done** — `deepcopy(vars)` replaced with shallow copy / copy-on-write |
| ~~P2~~ | ~~`laktory/models/basemodel.py:459`~~ | **Done** — cache key generation switched to `hashlib.sha256` on canonical serialisation |
| ~~P3~~ | ~~`laktory/yaml/recursiveloader.py:149`~~ | **Done** — `rglob("*")` replaced with `rglob("*.yaml")` + `rglob("*.yml")` |
| ~~P4~~ | ~~`laktory/narwhals_ext/dataframe/schema_flat.py:73`~~ | **Done** — depth guard added to recursive `get_fields()` |

---

## 3. Code Quality

Anti-patterns, dead code, deprecated usage, naming inconsistencies.

### Dead / commented-out code

| # | File | Issue |
|---|------|-------|
| ~~Q1~~ | ~~`laktory/models/datasources/basedatasource.py:47-71`~~ | **Done** — commented-out `broadcast` / `sample` blocks removed |
| ~~Q2~~ | ~~`laktory/narwhals_ext/dataframe/__init__.py`~~ | **Done** — `stream_join` removed |
| ~~Q3~~ | ~~`laktory/models/resources/databricks/mlflowwebhook.py:62-64`~~ | **Done** — commented-out stubs removed |
| ~~Q4~~ | ~~`laktory/models/resources/databricks/table.py:53`~~ | **False positive** — `__optional_fields__` is consumed by `ModelMetaclass.__new__` in `basemodel.py:99` to make inherited required fields optional in subclasses without re-declaring them |
| ~~Q5~~ | ~~`laktory/cli/_quickstart.py:62`~~ | **Done** — `dits` renamed to `dirs` |

### Mutable defaults

| # | File | Issue |
|---|------|-------|
| ~~Q6~~ | ~~`laktory/models/readerwritermethod.py:25-26`~~ | **Done** — mutable defaults replaced with `default_factory` |
| ~~Q7~~ | ~~`laktory/models/datasources/customreader.py:56-62`~~ | **Done** — mutable defaults replaced with `default_factory` |

### Duplication

| # | File | Issue |
|---|------|-------|
| ~~Q8~~ | `laktory/models/datasources/filedatasource.py:23` + `laktory/models/datasinks/filedatasink.py:16` | **Won't fix** — kept separate intentionally to allow sources and sinks to diverge on supported formats independently |
| ~~Q9~~ | ~~`laktory/typing.py` + `laktory/models/datasources/dataframedatasource.py:14`~~ | **Done** — duplicate `AnyFrame` alias removed; consolidated to `laktory.typing` |

### Style inconsistencies

| # | Issue |
|---|-------|
| ~~Q10~~ | **Done** — codebase-wide migration to `X \| Y` union syntax completed |
| ~~Q11~~ | `laktory/models/resources/databricks/*.py` (20+ files) | **Won't fix — by design.** Each `*_base.py` defines the main base class plus several nested helper classes. The wildcard brings all helpers into the child module's namespace so griffe can resolve field types for documentation (`griffe_fieldz include_inherited=True` + `__doc_generated_base__` splitting). The `# NOQA: F403 required for documentation` comment already explains this. |

---

## 4. Architecture & Design

Structural issues that affect correctness, extensibility, or future maintenance.

| # | File | Issue |
|---|------|-------|
| ~~A1~~ | ~~`laktory/models/basemodel.py:77-154`~~ | **Done** — tracked as known tech debt; `ModelMetaclass` behavior documented |
| ~~A2~~ | ~~`laktory/models/basemodel.py:331-356`~~ | **Done** — tracked as high-risk tech debt; `validate_assignment_disabled()` flagged for Pydantic upgrade monitoring |
| ~~A3~~ | ~~`laktory/models/stacks/stack.py:127-182`~~ | **Done** — resource registry pattern implemented |
| ~~A4~~ | ~~`laktory/models/stacks/terraformstack.py:121-145`~~ | **Done** — variable substitution switched to structured substitution |
| ~~A5~~ | ~~`laktory/models/resources/databricks/` (all)~~ | **Done** — `catalog_name` / `schema_name` / `volume_name` propagation consolidated |
| ~~A6~~ | ~~`laktory/models/resources/baseresource.py:350-352`~~ | **Done** — `depends_on` validated against stack resource keys |
| ~~A7~~ | ~~`laktory/models/resources/databricks/terraformresource.py` + `workspacetree.py:123`~~ | **Done** — container resources given distinct base class |
| ~~A8~~ | ~~`laktory/models/stacks/stack.py:401-426`~~ | **Done** — variable merge encapsulated in named method with documented precedence |
| ~~A9~~ | ~~`laktory/models/basechild.py:19`~~ | **Done** — `_parent` narrowed to `BaseModel \| None`; depth guard added to traversal |
| ~~A10~~ | ~~`laktory/models/resources/baseresource.py:161-193`~~ | **Done** — `base_lookup()` behavior documented; extraction tracked |
| ~~A11~~ | ~~`laktory/dab.py:92-94`~~ | **Done** — `/files/` DABs contract documented in `docs/concepts/dab.md` |

---

## 5. User Experience & Public API

Issues that make Laktory harder to use correctly.

| # | File | Issue |
|---|------|-------|
| ~~U1~~ | ~~`laktory/models/datasources/unitycatalogdatasource.py` + `unitycatalogdatasink.py`~~ | **Done** — `@model_validator` added to both classes; Polars backend rejected at construction time with `ValueError` |
| ~~U2~~ | ~~`laktory/models/resources/databricks/` (all securable resources)~~ | **Done** — rewrote `grant`/`grants` field descriptions across all 9 resource files (plain-language "when to use" replacing "authoritative" jargon); improved `Grant`/`Grants` standalone class docstrings; added "Grant Model" section to architectural_patterns.md |
| ~~U3~~ | ~~`laktory/models/resources/baseresource.py:283-292`~~ | **Done** — `ResourceOptions.name` description rewritten with algorithm, examples, cross-reference syntax, and override guidance; `resource_name` property docstring updated; "Resource Naming" section added to architectural_patterns.md |
| ~~U4~~ | ~~Multiple resources~~ | **Done** — `lookup_existing` field description updated on all 12 supporting resources with specific lookup key(s) and correct behaviour note; concept section added to `docs/concepts/iac.md` pointing users to per-resource API docs |
| ~~U5~~ | ~~`laktory/dab.py:130-152`~~ | **Done** — "Deployment strategies" section added to `docs/concepts/dab.md` explaining the two patterns: hybrid (DABs for workspace resources + Laktory stack for account-level/UC) and Laktory-only |
| ~~U6~~ | `laktory/models/resources/databricks/` | **False positive** — `ExternalLocation`, `StorageCredential`, and `MetastoreDataAccess` already have `grant`/`grants` fields with full `additional_core_resources` wiring |
| ~~U7~~ | ~~Codebase-wide~~ | **Done** — `docs/concepts/governance.md` rewritten: decision table added at top; Grants and Access Controls sections expanded with correct field names, when-to-use guidance, YAML examples, and API reference admonitions; stale `permissions:` field name replaced with `access_controls:` |
| ~~U8~~ | ~~`laktory/narwhals_ext/dataframe/with_row_index.py:64-65`~~ | **Done** — error message rewritten to explain why `order_by` is required (lazy evaluation has no row order guarantee) and show concrete fix examples |

---

## 6. Testing

Coverage gaps, brittle fixtures, missing test categories.

| # | File | Issue |
|---|------|-------|
| T1 | `tests/` | No dedicated tests for any CLI command (`build`, `deploy`, `preview`, `destroy`, `init`, `run`, `validate`) — only `test_quickstarts.py` exercises the CLI indirectly |
| T2 | `tests/conftest.py:49` | Hardcoded Azure Databricks endpoint `https://adb-2211091707396001.1.azuredatabricks.net/` — will break if the test environment changes |
| T3 | `laktory/_testing/__init__.py:36-84` | `_get_stack_resource_key()` uses `typing.get_args`/`get_origin` introspection to find resource types — will silently break if `StackResources` type signature changes |
| T4 | `tests/` | No performance / benchmark tests for large YAML loads, deep recursive schemas, or large pipelines — no way to detect regressions in the areas PR #552 fixed |
| T5 | `tests/` | `laktory.settings` is a module-level singleton; settings mutations in one test can leak into subsequent tests without explicit teardown |
| T6 | `tests/` | No tests for partial-failure recovery: YAML loading that fails mid-file, CLI commands receiving malformed input, etc. |
| T7 | `tests/narwhals_ext/` | Narwhals extension tests don't verify isolation — no test that registering Laktory namespaces doesn't conflict with user-registered namespaces or other Narwhals versions |

---

## 7. Strategic

Larger directional gaps or decisions that affect the project roadmap.

| # | Issue |
|---|-------|
| S1 | **DABs coverage is partial.** The Terraform path handles the full stack (UC, compute, pipelines, grants). The DABs path handles only pipelines and jobs. Users building greenfield environments cannot use DABs exclusively — they must learn both deployment systems. |
| S2 | **`StackResources` doesn't scale.** The monolithic class with 50 hardcoded fields is already large. Adding every new Databricks resource type requires a manual edit. A registry/factory pattern would make this extensible without touching the class. |
| S3 | **Narwhals extension globals.** Registering `laktory` namespaces onto `nw.Expr` / `nw.DataFrame` / `nw.LazyFrame` at import time is a global side effect. As Narwhals adds native features, these extensions may conflict or duplicate upstream. No mechanism exists to disable or version the extensions. |
| S4 | **Polars support is incomplete.** `UnityCatalogDataSource`, `UnityCatalogDataSink`, and streaming sinks all raise `NotImplementedError` for Polars. If Polars support is not intended, it should be removed from the public API and type signatures rather than failing at runtime. |
| S5 | **Permissions / Grants are two unrelated APIs.** From a user perspective both control access. The underlying Terraform/Databricks split (ACLs for compute vs. Unity Catalog privileges) is an implementation detail that leaks into the user model. A unified `access` abstraction that routes to the right resource internally would simplify stack definitions. |
