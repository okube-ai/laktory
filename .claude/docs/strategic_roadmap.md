# Laktory Strategic Roadmap

> **Archived** - all six items are complete as of May 2026. This document is historical reference only; no further action is needed.

_Updated May 2026. All six decisions are complete - no remaining work items._

---

## What Was Decided (for reference)

- **Dropped Pulumi** (PR #537): all ~50 resource classes now inherit only `TerraformResource`. `pulumiresource.py`, `pulumistack.py`, and all Pulumi serialization logic removed.
- **Declared Databricks first-class** (design): the codebase is officially Databricks-focused. The Narwhals dataframe layer stays platform-agnostic; the resource/IaC layer does not.
- **Added DABs as a deployment path** (PR #533): `laktory/dab.py` exposes `build_resources(bundle)` for the Databricks CLI. Jobs and DLT Pipelines can now be deployed via `databricks.yml` without Terraform.
- **Generated resource models from Terraform schema** (PR #536): `scripts/build_resources/` generates `*_base.py` files from `terraform providers schema -json`. Hand-maintaining 50+ resource fields is no longer needed.
- **Hardened `RecursiveLoader`** (PR #551): added `try/finally` variable-state restoration so `loader.variables` is always reset on error; circular-reference detection for both `!use` and `<<:` merge keys; improved error messages for missing `!use` targets; guarded `<<:` string replacement against false positives.
- **Fixed `inject_vars` performance** (PR #552): replaced `isinstance(Pipeline/PipelineNode)` checks (which forced circular imports on every call) with a `_inject_vars_objs()` override pattern on `Pipeline` and `PipelineNode`; added a cache keyed on serialized `vars` so repeated calls with unchanged variables skip re-resolution entirely.

---

## Open Item: SDP Lakeflow Job dual-mode path

The `SPARK_DECLARATIVE_PIPELINE` orchestrator is designed to serve two execution contexts with identical generated artifacts:

| Mode | Execution | Infrastructure |
|------|-----------|----------------|
| **Local** | `spark-pipelines run --spec …` via subprocess | Local PySpark 4.1+ |
| **Lakeflow Job** | Same script as a Databricks Job task | DBR 16.x; no DLT license required |

**Open question:** how the mode is selected (orchestrator field, deploy-time flag, or inferred from context) is TBD — blocked on testing the Databricks Job execution path.
