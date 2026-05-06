# Laktory Strategic Roadmap

_Updated May 2026. The four decisions from the March 2026 session are complete._

---

## What Was Decided (for reference)

- **Dropped Pulumi** (PR #537): all ~50 resource classes now inherit only `TerraformResource`. `pulumiresource.py`, `pulumistack.py`, and all Pulumi serialization logic removed.
- **Declared Databricks first-class** (design): the codebase is officially Databricks-focused. The Narwhals dataframe layer stays platform-agnostic; the resource/IaC layer does not.
- **Added DABs as a deployment path** (PR #533): `laktory/dab.py` exposes `build_resources(bundle)` for the Databricks CLI. Jobs and DLT Pipelines can now be deployed via `databricks.yml` without Terraform.
- **Generated resource models from Terraform schema** (PR #536): `scripts/build_resources/` generates `*_base.py` files from `terraform providers schema -json`. Hand-maintaining 50+ resource fields is no longer needed.

---

## Remaining Work

### 1. Fix `RecursiveLoader` robustness _(high risk, low effort)_

File: `laktory/yaml/recursiveloader.py`

- Add `try/finally` around `loader.variables` state mutation so variables are always restored on error
- Add circular reference detection to prevent infinite `!use` loops
- Improve error messages for missing `!use` targets
- Guard the `<<:` string replacement against false positives (currently a plain string replace)

### 2. Address `inject_vars` performance _(quality of life)_

- Profile `inject_vars` / `push_vars` with a realistic large pipeline
- The current implementation deep-copies on every call; replace with lazy or cached evaluation
