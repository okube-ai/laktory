# Laktory TODO

---

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

Currently opt-in (`settings.workspace_root: "user_root"` — see `docs/concepts/laktorysettings.md#workspace-root`). Promote it to the *default* `workspace_root` value once adoption/feedback validates the pattern, so a stack gets a collision-free, per-user/stack/env deployment root without any configuration.

**Note:** this changes deployed-object paths for every existing stack that doesn't already set `workspace_root` explicitly, so per semver it needs a **major** version bump (breaking change), not a minor one — file under `### Breaking changes` in the CHANGELOG with a migration note when it lands, and requires a `DatabricksProvider` in the stack (today's default doesn't).

## A5 — `${current_user.x}`: add a short-name/`alphanumeric` form

`${current_user.user_name}` shipped in #633 (`laktory/_current_user.py`, wired into `Stack._resolve_user_root`, documented in `docs/concepts/variables.md#current-user`). Not yet added: a "short name" form. The Databricks SDK's `User` object (`databricks.sdk.service.iam.User`) has no `short_name`/`alphanumeric` field — verified fields are `active, display_name, emails, entitlements, external_id, groups, id, name, roles, schemas, user_name`. Terraform's `databricks_current_user.alphanumeric` attribute is computed by the provider itself (Go code), not returned by the raw API. Before implementing, either (a) check the Databricks Terraform provider's source/docs to replicate that exact sanitization so it matches `${resources.x.alphanumeric}` if both appear in the same stack, or (b) derive Laktory's own convention (e.g. the part of `user_name` before `@`) and document it explicitly as not a claim of parity with Terraform's `alphanumeric`.
