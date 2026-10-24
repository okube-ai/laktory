# Plan — Laktory 1.0 (#697)

Status: proposed (2026-10-22). Current version: 0.13.2 (unreleased).

## Goal

Release 1.0.0 as a non-event: all known breaking changes land during 0.x, at least one minor
release ships without new breaking changes, and the public contract (what is covered by semver,
how deprecations work) is written down before the version number changes.

## Why not now

- **Track record**: 23 of the 26 releases in `CHANGELOG.md` have a non-empty "Breaking changes"
  section, including the 0.13.1 *patch* (default Terraform provider constraint raised to
  `>=1.137.0`). A stability promise needs a history of keeping it.
- **Known breaking items still open**: A4, A7, A9, A11 (`REPLACE` as default reset mode) and
  the A8 removal, all decided 2026-10-22/24 (see `todo.md`). A3 and A12 were dropped. Changing a
  default after 1.0 is a breaking change; adding a feature is not.

## Workstreams

### W1 — Release discipline (gate for 1.0)

- From 0.14 on, no breaking changes in patch releases. Raising the minimum Terraform provider
  version counts as breaking and only happens in a minor.
- **1.0 gate**: one minor release (target 0.15) whose "Breaking changes" section only contains
  removals announced in a previous release.

### W2 — Land the breaking changes in 0.14

| Item | Change | Notes |
|------|--------|-------|
| A7 | Discriminated unions for sinks, sources, transformer nodes, orchestrator | Prototyped 2026-10-22 (11-24 errors -> 1). Not breaking for valid configs (key inference mirrors required fields): error format and union aliases change. Parse pass over all repo YAML |
| A9 | `purge()` -> `reset(mode=None)` on sinks, nodes, pipeline; `purge_target` -> `reset_target`; private helpers renamed | Decided 2026-10-22: deprecated aliases in 0.14, removed in 0.15 |
| A4 | Scoped default `workspace_root` `/.laktory/{stack}/{env}/` + per-environment `settings` (enables `user_root` for selected envs) | Decided 2026-10-22: `user_root` stays opt-in. Migration note: `workspace_root: /.laktory/` keeps the old layout |
| A8 | Remove legacy `full_refresh` everywhere (Python API, job script, Airflow, reserved name) + rename SDP orchestrator `execute(full_refresh)` to `refresh` | Decided 2026-10-22, 0.15. Old jobs/triggers passing `full_refresh=true` will then run incrementally silently: explicit CHANGELOG upgrade note |

Under 0.x a minor may break, so A4 (flagged "major only") goes in 0.14 rather than 1.0. A12 (strict name prefix) was dropped on 2026-10-23: see `todo.md`.

### W3 — Deprecation mechanism

- Today deprecation warnings are ad hoc (`laktory/models/pipeline/pipelinenode.py`,
  `laktory/models/resources/baseresource.py`, `laktory/yaml/recursiveloader.py`).
- Add a single helper, e.g. `laktory._deprecation.warn_deprecated(name, since, removed_in,
  replacement)`, emitting a `DeprecationWarning`/`FutureWarning` with a consistent message.
  Migrate existing call sites.
- Policy: a deprecated feature stays at least one minor release with a warning and a CHANGELOG
  entry under a new `### Deprecated` heading, then is removed in a later minor (or the next
  major once in 1.x).

### W4 — Decide defaults before the freeze

- **A11** (decided 2026-10-24): `reset_mode: REPLACE` added **and** made the default for Delta
  table sinks in **0.14** (0.15 is the no-new-breaking rehearsal). Release gate: e2e on Databricks
  showing grants, tags, a synced table and a streaming reader surviving a full refresh.
- **A3**: dropped (2026-10-24): no SDP Lakeflow Job mode; the SDP orchestrator stays local /
  non-Databricks execution. See `todo.md`.
- **A13** (decided 2026-10-24): `runtime_root` single default `/laktory/` (bug #716, breaking,
  0.14) + run-time guided error on `DBFS_DISABLED` (additive). No build-time failure: legacy
  workspaces still have DBFS.
- **A14**: quickstarts on a Unity Catalog Volume so they run as-is on new workspaces (adoption,
  not a blocker).
- Not blockers (additive or bug fixes): #680, #710, A2, A5, A10.

### W5 — Public API definition

Write a `docs/concepts/stability.md` (or similar) page stating what is covered by semver:

- **Covered**: YAML model schema of Laktory-owned models (pipeline, nodes, sources, sinks,
  transformers, stack, settings), Laktory-specific fields on resources (nested `grants`, `tags`,
  `access_controls`, ...), the `laktory` CLI (commands, options, exit behavior), top-level Python
  API (`laktory` / `laktory.models` exports), job / run parameters, variable namespaces
  (`${vars}`, `${resources}`, `${params}`, ...).
- **Not covered**: resource fields generated from the Databricks Terraform provider follow the
  provider (fields added, renamed or removed upstream are not Laktory breaking changes); private
  modules (`_`-prefixed); generated artifacts' internal layout.
- **Provider policy**: minimum provider version raised only in minors.
- **Default resource names** (state addresses): the current prefix rule is frozen and documented
  (A12 dropped).
- **Source / sink kind inference** (A7): inference from keys is frozen to the kinds existing at
  1.0; new kinds must declare `type`.

### W6 — Dependency baseline

- Python 3.10 reaches end of life in October 2026: drop it in 0.14 (`requires-python >= 3.11`).
- Review upper pins (`polars<1.32.1`, `pyspark` dev pin `<4.2.0`) and remove the ones that are no
  longer needed; strict upper bounds in a 1.x library create resolver conflicts for users.

### W7 — Versioned docs (#696)

- Independent, ~1-2 days. Ship during 0.14 so 0.13 / 0.14 docs exist before 1.0.
- `mike`, one version per minor, `latest` alias, root `404.html` redirect, keep `CNAME`.

### W8 — 1.0 release

- Migration guide `docs/migration/0.x-to-1.0.md` (collects W2 upgrade notes).
- CHANGELOG entry, release announcement (log in
  `../okube-growth/strategy/content_calendar.md`).

## Sequence

| Release | Content |
|---------|---------|
| 0.14 | W2 (A7, A9 with alias, A4), A11 `REPLACE` default, A13 / #716 runtime root, W3 helper, W6 Python 3.11+, W7 versioned docs, W4 decisions |
| 0.15 | Rehearsal: no new breaking changes. A8 removal (announced), W5 stability page, draft migration guide |
| 1.0 | 0.15 + finalized migration guide and announcement |

## Open questions

- Backport policy after 1.0: fixes on the previous minor only, or none?
