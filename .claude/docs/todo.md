# Laktory TODO

---

## A2 — AI-first solution

How can we offer an AI-first solution? Agents that understand lineage and propose solutions from natural language.

## A3 — SDP Lakeflow Job dual-mode path (DROPPED)

**Decision (2026-10-24, #697): dropped.** Running the `SPARK_DECLARATIVE_PIPELINE` artifacts as a
Databricks Job task adds little: the cost-saving path on Jobs compute already exists
(`LakeflowJobOrchestrator`), and SDP in a Job loses what makes declarative pipelines worth it on
Databricks (incremental materialized view refresh, managed UC streaming tables / MVs, event log,
expectations UI, serverless) - users wanting those use LDP. Runtime support for OSS
`spark-pipelines` on Databricks was also unverified. Revisit only on user demand.

The SDP orchestrator is positioned as local / non-Databricks execution (OSS Spark 4.1+) with the
same behavior as LDP, e.g. to test a declarative pipeline locally before deploying it with
`LakeflowDeclarativePipelineOrchestrator` (makes #680 more important than A3).

## A4 — Scoped default `workspace_root` and per-environment `user_root`

**Decision (2026-10-22, #697):** `user_root` does *not* become the default. It makes deployed
paths depend on who deploys (CI service principal vs developer), needs a live SDK call and a
`DatabricksProvider` on every build, and doesn't apply to DAB.

Instead, for 0.14:

1. **Scoped default**: inside a stack, `workspace_root` defaults to
   `/.laktory/{stack_name}/{env_name}/` (`/.laktory/{stack_name}/` without env). No network
   call, deterministic, same path whoever deploys. Standalone pipelines (no stack) keep
   `/.laktory/`. Breaking (deployed paths move): CHANGELOG `### Breaking changes` + migration
   note (`workspace_root: /.laktory/` restores the old layout).
2. **Per-environment settings**: add `settings` to `EnvironmentSettings`, merged field by field
   over the stack `settings` in `Stack._apply_env_overrides` (like `variables`, not a full
   replace), so `user_root` can be enabled for `dev` only. Resource paths read the
   `settings` singleton lazily (`Notebook.path`, `Dashboard`, ...), so applying the merged env
   settings before `_resolve_user_root` is enough.
3. **`${vars.x}` -> `user_root`**: resolve variables in `settings.workspace_root` before the
   sentinel check in `_resolve_user_root` (today the check sees the raw template; unverified).

```yaml
settings:
  runtime_root: /Volumes/main/laktory/runtime/
environments:
  dev:
    settings:
      workspace_root: user_root   # /Users/{you}/.laktory/{stack}/dev/
  prod: {}                        # /.laktory/{stack}/prod/
```

**Done (2026-10-25, branch `feat/a4-scoped-workspace-root`):** all three items, in
`Stack._resolve_workspace_root` (renamed from `_resolve_user_root`). Notes:
- `LaktorySettings.workspace_root` defaults to `None` so "unset" is detectable; the scoped
  default is skipped when `LAKTORY_WORKSPACE_ROOT` is set in the environment.
- Env settings are `EnvironmentLaktorySettings` (`_push_to_singleton = False`): not pushed onto
  the singleton at stack construction, only once merged for the selected env.
- Fixed on the way: `get_env()` mutated the stack (`get_env(None)` returned `self`; env
  `variables`/`settings` merged in place into the shallow copy's shared objects), so building
  several envs in one process leaked the first one into the next.
- Still open (pre-existing): env `resources` are merged in place into the stack's
  `StackResources` dicts, so they leak across envs built in one process; env-level `providers`
  aren't routed to typed providers (`EnvironmentSettings.resources: Any`). A deep copy breaks
  pipeline/orchestrator `_parent` links.
- Workspace file resource names include the root (`workspace-file-laktory-{stack}-{env}-...`),
  so files are recreated, not moved, on upgrade.

## A5 — `${current_user.short_name}` (DONE)

**Done (2026-10-26):** shipped as `${current_user.short_name}` with the Declarative Automation Bundles algorithm (`GetShortUserName` in `databricks/cli` `libs/iamutil/user.go`: part before `@`, lowercase, non letter/digit -> `_`, collapse and trim `_`, service principal -> display name when set), not Terraform's `alphanumeric` (ASCII only, no collapse/trim, service principal -> application id). Chosen for parity with bundles, which Laktory users are more likely to use alongside a stack, and for cleaner names. `CurrentUser.short_name` in `laktory/_current_user.py`; `display_name` comes from the same `current_user.me()` call.


`${current_user.user_name}` shipped in #633 (`laktory/_current_user.py`, wired into `Stack._resolve_user_root`, documented in `docs/concepts/variables.md#current-user`). Not yet added: a "short name" form. The Databricks SDK's `User` object (`databricks.sdk.service.iam.User`) has no `short_name`/`alphanumeric` field — verified fields are `active, display_name, emails, entitlements, external_id, groups, id, name, roles, schemas, user_name`. Terraform's `databricks_current_user.alphanumeric` attribute is computed by the provider itself (Go code), not returned by the raw API. Before implementing, either (a) check the Databricks Terraform provider's source/docs to replicate that exact sanitization so it matches `${resources.x.alphanumeric}` if both appear in the same stack, or (b) derive Laktory's own convention (e.g. the part of `user_name` before `@`) and document it explicitly as not a claim of parity with Terraform's `alphanumeric`.

## A7 — Discriminated unions for sinks, sources and nodes

A validation error in a sink is buried among the errors of every union member (one per sink type
plus the `${...}` variable placeholder string), with unrelated errors first and very long error
paths. Measured 2026-10-22: `mode: OVERWRITE` + `shared: true` -> 11 errors, a typo'd sink field
-> 13 errors, a bad `PipelineNodeDataSource` field -> 24 errors.

**Prototype (scratch, 2026-10-22):** `Annotated[Union[Annotated[X, Tag("X")], ...,
Annotated[VariableType, Tag("var")]], Discriminator(fn)]` reduces each case to **1 error** with a
short path (`sinks.0.UnityCatalogDataSink.modee`). Unknown `type` gives a single
`union_tag_invalid` listing the valid tags.

**Design:**
- Tag function: `str` -> variable; model instance -> its class (`isinstance`, so subclasses
  work); dict with an explicit `type` (and not a `${...}` placeholder) -> that type; otherwise
  infer from keys:
  - sinks: `pipeline_view_name` -> view, `path` -> file, else -> Unity Catalog table
  - sources: `node_name` -> node, `custom_reader` -> custom, `df` / `data` -> dataframe,
    `path` -> file, else -> Unity Catalog table
  - transformer nodes: `expr` -> `DataFrameExpr`, else `DataFrameMethod`
  - `Pipeline.orchestrator`: `type` (all orchestrators have a `type` literal; the commented-out
    `discriminator="type"` in `pipeline.py` failed because `type` gets `| VariableType`)
- `_inject_variable_types` (`laktory/models/basemodel.py`) must not wrap an already
  discriminated union in `| VariableType` (it would bring back a second error): the tagged union
  carries `VariableType` as its own member.
- Keys inference mirrors each class's required fields, so every config valid today selects the
  same class: **not breaking for valid configs**. What changes: error messages, and the shape of
  `DataSinksUnion` / `DataSourcesUnion` for Python users introspecting them. Not a 1.0 blocker,
  but ship in 0.14 with a parse pass over all YAML in `tests/`, `docs/` and quickstart stacks,
  variables / `inject_vars` re-validation, and the MCP / docs rendering of the annotations.

**Design choice (2026-10-22):** keep a single, ordered, mixed list (order matters: the first
source is the transformer's `{df}`), with key inference for today's kinds. Rejected:
per-kind lists (`file_data_sources`, ...) - loses ordering / primary source, mixed kinds are
the norm, two new node fields per kind. An explicit kind (required `type` or a wrapper key like
`- table: {...}`) would be the from-scratch choice (clean errors, precise JSON schema for IDEs and
agents) but breaks every config for little gain over A7. Rules:
- `type` accepted on every source / sink, shown in reference examples.
- **New source / sink kinds must declare `type`**; inference stays frozen to today's kinds
  (a Lakebase / Postgres / JDBC kind would share `table_name` and be misread as Unity Catalog).
- Making `type` required stays possible as a deprecated-then-removed change for a 2.0.

## A8 — Remove the legacy `full_refresh` argument

`full_refresh` was replaced by `refresh` in 0.13.0 (2026-09-28). It is still accepted, only to fail
with a guided error when `True` (warning when `False`), via `check_legacy_full_refresh`
(`laktory/models/pipeline/_execute.py`).

**Decision (2026-10-22, #697): remove everything in 0.15:**
- `full_refresh` argument of `Pipeline.execute()` and `PipelineNode.execute()` (-> `TypeError`)
- `--full_refresh` argument of the `_execute` job script
- Airflow orchestrator `conf["full_refresh"]` / `params["full_refresh"]` check
- `check_legacy_full_refresh` itself, and `full_refresh` from `RESERVED_PARAMETER_NAMES`
  (`laktory/models/pipeline/pipeline.py`)
- `SparkDeclarativePipelineOrchestrator.execute(full_refresh=...)` renamed to `refresh`
  (Laktory naming everywhere)
- `laktory/AGENTS.md` and docs mentions

Out of scope: `DatabricksPipelineRunner.run(full_refresh=...)` (dispatcher, maps to the Databricks
`start_update` API) and generated `job_base.py` fields (Terraform provider).

Known consequence, accepted: the job script uses `parse_known_args()` and Airflow ignores unknown
`conf` keys, so a job / trigger still passing `full_refresh=true` after the removal runs
**incrementally without error**. CHANGELOG `### Breaking changes` entry must say so explicitly:
redeploy jobs deployed before 0.13.0 and update triggers / Airflow `conf` passing `full_refresh`.

## A9 — `purge()` -> `reset()` naming

Since 0.13.0 the docs and run parameters say "reset" (`refresh="RESET"`, `reset_mode`), but the
methods are still `purge`.

**Decision (2026-10-22, #697):**
- **0.14**: rename to `reset(mode=None)` with the same signature on `BaseDataSink` (and the file /
  table / pipeline view overrides), `PipelineNode` and `Pipeline` (drops the unused `spark`
  argument of `Pipeline.purge(spark=None)`, adds `mode`). `purge()` kept as a deprecated alias
  (first user of the W3 `warn_deprecated` helper) calling `reset()`.
- Rename the `purge_target` property to `reset_target` (public in practice; deprecated alias
  property too), and the private helpers (`_get_purge_mode`, `_purge_checkpoint`,
  `_purge_shared_data`, `_purge_other_writers_checkpoints`, `purge_checkpoint()`) without alias.
- **0.15**: remove the `purge()` / `purge_target` aliases, so 1.0 starts clean.
- Keep public `reset()` (not only `execute(refresh="RESET")`): resetting a single sink outside a
  run is useful and `pl.reset()` is discoverable.
- ~26 `.purge(` call sites in `laktory/`, `docs/`, `tests/`.

## A10 — Streaming readers of shared sinks: automatic handling

A writer's full refresh deletes rows from a shared table, failing its streaming readers
(`DELTA_SOURCE_IGNORE_DELETE`). 0.13.0 documents the choice (`docs/concepts/sharedsinks.md`,
"Streaming Readers"): full refresh of the reader, or `skipChangeCommits` on a `node_name` reader
(immune to other writers' refreshes, duplicates after its own writer's refresh). Verified locally
(Spark, delta-spark 4.2).

Candidate improvement: set `skipChangeCommits` automatically on `node_name` stream reads of a
shared sink, and cascade a full refresh of a writer to its downstream streaming readers in the same
run (`refresh=FULL` on `feed_a` also resets the checkpoints / sinks of `gld_feed_a`). Needs a
design for job task selections (the reader may not be part of the run).

## A11 — `REPLACE` reset mode (default for Delta tables)

`reset_mode` supports `DROP` (default) and `TRUNCATE`. `DROP` on a Unity Catalog table creates a
new table object: grants / tags set outside Laktory, comments, history and the **table identity**
are lost, breaking what references it by id (Lakebase synced tables #710, data profiling
monitors, Delta Sharing, downstream streaming readers). Databricks recommends
`CREATE OR REPLACE TABLE`.

**Decision (2026-10-24, #697): add `reset_mode: REPLACE` and make it the default for Delta table
sinks (Unity Catalog and Hive) in 0.14.** Other sinks keep `DROP`. 0.14 rather than 0.15 because
0.15 is the no-new-breaking rehearsal release.

Design:
- On reset, `CREATE OR REPLACE TABLE <t> (<schema of the DataFrame to write>)` (empty, new
  schema), then write normally. Same path for batch and streaming (schema known before the first
  micro-batch).
- `refresh=RESET` (no write): replace with the table's current schema (identity-preserving
  truncate).
- Delta tables only (native support); not file sinks, not declarative orchestrators.
- Shared sinks unchanged (they delete their writer's rows).
- Document: old files are kept until `VACUUM` (unlike `DROP`).
- CHANGELOG `### Breaking changes`: full refresh of Delta tables now keeps the table;
  `reset_mode: DROP` restores the former behavior.

**Release gate:** e2e on Databricks (`e2e-test` skill): grants, tags, a `PostgresSyncedTable` and a
streaming reader survive a full refresh; schema change on full refresh works; local Spark +
delta-spark tests.

## A12 — Default resource name prefix: strict match (DROPPED)

**Decision (2026-10-23, #697): dropped.** The strict `{resource_type_id}-` prefix check would
rename very common default names (`dev_catalog` -> `catalog-dev_catalog`, schema `raw_schema` ->
`schema-dev-raw_schema`, `job_ingest` -> `job-job_ingest`, `/notebooks/` files): destroy /
recreate of catalogs, schemas and volumes plus broken `${resources.<old>}` references, for a
cosmetic gain. Any rule change (even a token-based match) changes state addresses.

Instead: document the current rule exactly (substring match in
`BaseResource._resource_safe_key_has_type_id`; `File` uses the strict check since 0.13.1, kept as
is) in the 1.0 stability page - the default name is the state address, so it is part of the
contract. Escape hatch: `resource_options.name`.

## A13 — `runtime_root`: guided error without DBFS, single default

`runtime_root` is the root of Spark structured-streaming checkpoints (sinks, node expectations).
DBFS is still supported on serverless but **not on new workspaces**, and the configuration can't
tell a legacy workspace from a new one: no build-time failure.

**Decision (2026-10-24, #697):**
1. **Run-time guided error (additive, any release)**: catch `[DBFS_DISABLED]` on checkpoint
   writes and re-raise with "this workspace has no DBFS: set `settings.runtime_root` to a Unity
   Catalog Volume, e.g. `/Volumes/<catalog>/<schema>/<volume>/laktory/`".
2. **Single default (breaking, 0.14)** - bug [#716](https://github.com/okube-ai/laktory/issues/716):
   the default checkpoint root depends on whether the stack has a `settings:` block
   (`/.laktory/` without, `/laktory/` with, verified). Align on the documented `/laktory/`: stack
   `settings.runtime_root` defaults to `None`, `Pipeline.root_path` substitutes `/laktory/` for
   Databricks orchestrators, SDP (local) keeps `./.laktory/`. Upgrade note: set `runtime_root`
   explicitly to keep existing checkpoints.
   **Done (#716): aligned on `/.laktory/` instead** - the default since v0.10.0 for stacks without
   a `settings:` block (all quickstarts, DAB), consistent with `workspace_root` and `./.laktory/`.
   Only stacks with a `settings:` block not setting `runtime_root` move (from `/laktory/`).
3. **Follow-up - scope checkpoints by stack / env (decide with A4, 0.14)**: the root is
   `{runtime_root}/pipelines/{pipeline}/`, so dev and prod of a stack sharing a workspace with
   the same pipeline name share streaming checkpoints. Scoping like A4
   (`/.laktory/{stack}/{env}/pipelines/{pipeline}/`) fixes it, but moving checkpoints restarts
   every stream (unlike A4's deployed files): if done, a single checkpoint move for everyone.

## A14 — Quickstarts on a Unity Catalog Volume

Quickstarts must run as-is on any workspace, but the `workflows` quickstart depends on DBFS for its
data (`DbfsFile` upload, sources `dbfs:/laktory/data/stock_prices/` in `pl-stocks-job.yaml` /
`pl-stocks-ldp.yaml`), so it fails on new (DBFS-less) workspaces regardless of `runtime_root`.

**Decision (2026-10-24, #697):** quickstarts declare their own `Volume` under `${vars.catalog}`
(not a hard-coded `main`: new workspaces have a catalog named after the workspace), upload data
there and point `runtime_root` at it. Works on legacy and new Unity Catalog workspaces (non-UC
workspaces not covered, accepted). Not a 1.0 blocker, but an adoption one: verify each quickstart
e2e on a new workspace.
