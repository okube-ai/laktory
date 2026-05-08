# Laktory Codebase Audit — Findings

_Audited May 2026. Three parallel agents reviewed the full codebase: core models, infrastructure/resources, and utilities/YAML/CLI/tests. All findings verified against source._

---

## 1. Critical / Immediate

Issues that should be fixed before the next release — production bugs, footguns, or deprecated calls that are already incorrect.

| # | File | Issue |
|---|------|-------|
| C1 | `laktory/models/datasources/tabledatasource.py:137` | `print("Adding method", m.name)` debug statement pollutes stdout in production — replace with `logger.debug()` or remove |
| C2 | `laktory/models/grants/tablegrant.py:23` | `t = TableGrant(principal="a", privileges=["SELECT"])` is a module-level test instance that executes on every import — remove |
| C3 | `laktory/cli/_common.py:77` | `logger.warn()` deprecated since Python 3.2 — replace with `logger.warning()` |
| C4 | `laktory/cli/_common.py:135,148` | `print()` statements in CLI error-handling paths — route through logger for consistent output control |
| C5 | `laktory/version.py:52` | Bare `except:` (noqa'd) swallows `SystemExit` and `KeyboardInterrupt` — catch `ModuleNotFoundError` specifically |
| C6 | `laktory/models/datasinks/mergecdcoptions.py:512` | `except Exception` too broad; catches `AttributeError`, `ImportError`, etc. silently — **won't fix**: different Spark versions (including Spark Connect) raise different exception types for a missing table; narrowing risks swallowing a legitimate "table not found" on an untested version |

---

## 2. Performance

Measurable or clearly avoidable overhead on hot paths.

| # | File | Issue |
|---|------|-------|
| P1 | `laktory/models/basemodel.py:445,450,555` | `deepcopy(vars)` on every `inject_vars()` call even when `vars` is read-only; use shallow copy or copy-on-write |
| P2 | `laktory/models/basemodel.py:459` | Cache key built via `json.dumps(..., default=repr)` — `repr()` on arbitrary objects is slow and non-deterministic; use `hashlib.sha256` on a canonical serialisation |
| P3 | `laktory/yaml/recursiveloader.py:149` | `rglob("*")` then filter by extension string; use `rglob("*.yaml")` + `rglob("*.yml")` directly |
| P4 | `laktory/narwhals_ext/dataframe/schema_flat.py:73` | Recursive `get_fields()` has no depth guard — pathologically nested schemas cause stack overflow |

---

## 3. Code Quality

Anti-patterns, dead code, deprecated usage, naming inconsistencies.

### Dead / commented-out code

| # | File | Issue |
|---|------|-------|
| Q1 | `laktory/models/datasources/basedatasource.py:47-71` | Large `broadcast` / `sample` blocks commented out with no plan — remove or open a tracking issue |
| Q2 | `laktory/narwhals_ext/dataframe/__init__.py` | `stream_join` fully commented out — complete it or formally remove it |
| Q3 | `laktory/models/resources/databricks/mlflowwebhook.py:62-64` | Commented-out `terraform_renames` / `terraform_excludes` stubs — remove |
| ~~Q4~~ | ~~`laktory/models/resources/databricks/table.py:53`~~ | **False positive** — `__optional_fields__` is consumed by `ModelMetaclass.__new__` in `basemodel.py:99` to make inherited required fields optional in subclasses without re-declaring them |
| Q5 | `laktory/cli/_quickstart.py:62` | Loop variable named `dits` — typo for `dirs` |

### Mutable defaults

| # | File | Issue |
|---|------|-------|
| Q6 | `laktory/models/readerwritermethod.py:25-26` | `Field([])` / `Field({})` mutable defaults — use `default_factory=list` / `default_factory=dict` |
| Q7 | `laktory/models/datasources/customreader.py:56-62` | Same mutable-default pattern for `func_args` / `func_kwargs` |

### Duplication

| # | File | Issue |
|---|------|-------|
| ~~Q8~~ | `laktory/models/datasources/filedatasource.py:23` + `laktory/models/datasinks/filedatasink.py:16` | **Won't fix** — kept separate intentionally to allow sources and sinks to diverge on supported formats independently |
| Q9 | `laktory/typing.py` + `laktory/models/datasources/dataframedatasource.py:14` | `AnyFrame` type alias defined in both places — use the one from `laktory.typing` everywhere |

### Style inconsistencies

| # | Issue |
|---|-------|
| Q10 | Mixed `Union[X, Y]` (old) vs. `X \| Y` (Python 3.10+) syntax throughout the codebase — standardise on `X \| Y` |
| ~~Q11~~ | `laktory/models/resources/databricks/*.py` (20+ files) | **Won't fix — by design.** Each `*_base.py` defines the main base class plus several nested helper classes. The wildcard brings all helpers into the child module's namespace so griffe can resolve field types for documentation (`griffe_fieldz include_inherited=True` + `__doc_generated_base__` splitting). The `# NOQA: F403 required for documentation` comment already explains this. |

---

## 4. Architecture & Design

Structural issues that affect correctness, extensibility, or future maintenance.

| # | File | Issue |
|---|------|-------|
| A1 | `laktory/models/basemodel.py:77-154` | `ModelMetaclass.__new__` modifies field type annotations at class-definition time (VariableType injection, `__optional_fields__` expansion) — fragile to Pydantic internals and invisible to static type checkers; consider a cleaner registration mechanism |
| A2 | `laktory/models/basemodel.py:331-356` | `validate_assignment_disabled()` pokes `__pydantic_setattr_handlers__` (private Pydantic internals) — will break on Pydantic upgrades; track as high-risk tech debt |
| A3 | `laktory/models/stacks/stack.py:127-182` | `StackResources` has ~50 hardcoded resource-type fields; every new resource type requires editing this class — consider a resource registry / auto-registration pattern |
| A4 | `laktory/models/stacks/terraformstack.py:121-145` | Variable substitution via `json.dumps` + regex string replace on the entire serialised output — fragile to resource names or values containing `$` or regex-special characters; use structured substitution |
| A5 | `laktory/models/resources/databricks/` (all) | No `HierarchicalResource` base class — `catalog_name` / `schema_name` / `volume_name` propagation managed manually in each of ~15 classes; easy to get wrong |
| A6 | `laktory/models/resources/baseresource.py:350-352` | `depends_on: list[str]` is free-form — no validation that referenced resources exist in the stack; errors only surface at Terraform apply time |
| A7 | `laktory/models/resources/databricks/terraformresource.py` + `workspacetree.py:123` | `terraform_resource_type` declared `@abstractmethod` returning `str`, but `WorkspaceTree` returns `None` — breaks the type contract; give container resources a distinct base class |
| A8 | `laktory/models/stacks/stack.py:401-426` | Environment ↔ stack variable merge semantics are implicit (`model_copy` + `update`) — unclear precedence order; document and encapsulate in a named method |
| A9 | `laktory/models/basechild.py:19` | `_parent: Any` untyped — narrow to `BaseModel \| None`; `pipelinechild.py:78-108` traverses `_parent` chain with no depth guard |
| A10 | `laktory/models/resources/baseresource.py:161-193` | `base_lookup()` validator silently overwrites user-provided field values when `lookup_existing` is set — undocumented magic; extract to a factory classmethod |
| A11 | `laktory/dab.py:92-94` | `/files/` path in DABs workspace root is a hardcoded magic string — document the DABs contract or make it configurable |

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
| U6 | `laktory/models/resources/databricks/` | `ExternalLocation`, `StorageCredential`, and `MetastoreDataAccess` lack `grant`/`grants` fields despite being securable in Databricks — incomplete permissions surface |
| U7 | Codebase-wide | `Permissions` (compute objects) vs. `Grant`/`Grants` (UC objects) solve the same user need through completely different APIs with no shared documentation or abstraction |
| U8 | `laktory/narwhals_ext/dataframe/with_row_index.py:64-65` | `ValueError` raised when `order_by=None` on a LazyFrame gives no hint about workarounds or documentation links |

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
