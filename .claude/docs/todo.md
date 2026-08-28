# Laktory TODO

---

## A1 — MCP server ✅

`laktory/AGENTS.md`, `laktory/mcp/` server (4 tools + `get_laktory_docs`), `laktory serve-mcp` CLI, `laktory setup-agent` CLI, `.mcp.json` in all quickstart stacks.

## A2 — AI-first solution

How can we offer an AI-first solution? Agents that understand lineage and propose solutions from natural language.

## A3 — SDP Lakeflow Job dual-mode path

The `SPARK_DECLARATIVE_PIPELINE` orchestrator is designed to serve two execution contexts with identical generated artifacts:

| Mode | Execution | Infrastructure |
|------|-----------|----------------|
| **Local** | `spark-pipelines run --spec …` via subprocess | Local PySpark 4.1+ |
| **Lakeflow Job** | Same script as a Databricks Job task | DBR 16.x; no DLT license required |

**Open question:** how the mode is selected (orchestrator field, deploy-time flag, or inferred from context) is TBD — blocked on testing the Databricks Job execution path.

## A4 — Make `workspace_root: "user_root"` the default

Currently opt-in (`settings.workspace_root: "user_root"` — see `docs/concepts/workspaceroot.md`). Promote it to the *default* `workspace_root` value once adoption/feedback validates the pattern, so a stack gets a collision-free, per-user/stack/env deployment root without any configuration.

**Note:** this changes deployed-object paths for every existing stack that doesn't already set `workspace_root` explicitly, so per semver it needs a **major** version bump (breaking change), not a minor one — file under `### Breaking changes` in the CHANGELOG with a migration note when it lands, and requires a `DatabricksProvider` in the stack (today's default doesn't).

## A5 — `${current_user.x}` variable namespace

Expose the current Databricks identity (already fetched via a live SDK call for `workspace_root: "user_root"` / `backend.databricks_workspace`, see `_resolve_databricks_provider_and_username()` in `laktory/models/stacks/stack.py`) as reusable variables, at least `${current_user.user_name}` and a "short name" form.

**Why not just reuse the `CurrentUser` / `databricks_current_user` Terraform data source?** Not equivalent — that resolves at `terraform plan`/`apply` time (too late for the state backend, which must be known before Terraform even initializes, and too late for `workspace_root`'s non-Terraform, Python-side consumers like local file staging under `settings.build_root`).

**Design direction (agreed, not yet implemented):** a new, explicit, opt-in `${current_user.x}` template namespace mirroring `${settings.x}` — resolved lazily, only when actually referenced somewhere in the stack, requiring a `DatabricksProvider` only at that point. Not auto-injected into every stack's variables, to avoid adding a surprise live network call to deploys that don't ask for it.

**Open sub-question:** the Databricks SDK's `User` object (`databricks.sdk.service.iam.User`) has no `short_name`/`alphanumeric` field — verified fields are `active, display_name, emails, entitlements, external_id, groups, id, name, roles, schemas, user_name`. Terraform's `databricks_current_user.alphanumeric` attribute is computed by the provider itself (Go code), not returned by the raw API. Before implementing a "short name", either (a) check the Databricks Terraform provider's source/docs to replicate that exact sanitization so it matches `${resources.x.alphanumeric}` if both appear in the same stack, or (b) derive Laktory's own convention (e.g. the part of `user_name` before `@`) and document it explicitly as not a claim of parity with Terraform's `alphanumeric`.
