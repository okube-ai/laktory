---
name: e2e-test
description: This skill should be used when the user wants to validate a laktory feature or fix on a real Databricks workspace - "test this live", "set up an e2e stack", "deploy a test stack for this feature", "check it works on Databricks", or when a change (LDP/SDP pipelines, jobs, sinks, resources) can't be verified by the local pytest suite. Scaffolds a throwaway, git-ignored laktory stack under scratch/e2e/<slug>/, deploys it with the developer's CLI auth, runs and checks it, records findings, and tears it down.
---

Validate a laktory change end-to-end on the dev Databricks workspace with a
throwaway stack: scaffold, deploy, run, check, report, tear down.

Run inline, not as a background agent: the plan (step 2) and the teardown
(step 7) both wait for the user.

Reference example of a finished e2e stack: shared sinks with LDP append flows
(#676), `scripts/ldp_shared_sink/` if still present.

## Fixed environment

| What | Value |
|------|-------|
| Location | `scratch/e2e/<slug>/` (git-ignored via `scratch/*`); repo root is `../../../` from there |
| Auth | Databricks CLI profile `laktory-dev-cli` (`providers.databricks.profile`, and `-p laktory-dev-cli` on every `databricks` call) |
| Catalog / schema | `laktory` / `unit_tests` |
| Tables | prefixed `e2e_<prefix>_` (prefix: 2-4 letters from the slug) |
| DBFS data | `dbfs:/laktory/e2e/<slug>/` |
| Workspace files | `settings.workspace_root: user_root` → `/Users/{you}/.laktory/e2e-<slug>/dev/` |
| Stack / pipeline / job names | `e2e-<slug>`, `pl-e2e-<slug>`, `job-e2e-<slug>` |
| IaC | Terraform, local state in the stack folder |
| Commands | `uv run laktory ...` from the stack folder, so the working tree's laktory is used |

Everything the test creates must carry the slug/prefix: that's what makes
teardown and leftover detection reliable, and keeps concurrent e2e stacks
apart.

## Step 1 - Pre-flight

- `databricks auth profiles | grep laktory-dev-cli` must show `YES`. If not,
  ask the user to run `! databricks auth login -p laktory-dev-cli`.
- `git branch --show-current` and `git rev-parse --short HEAD`, for the README.
- Read `laktory/_version.py` for the wheel filename.
- Find a SQL warehouse for checks: `databricks warehouses list -p laktory-dev-cli`
  and use the serverless "laktory" warehouse. Don't use "Starter Warehouse": it's a classic
  warehouse that can't start (Azure VM quota). It starts on first query: while it does, the
  statement API returns `PENDING` - poll `GET /api/2.0/sql/statements/<statement_id>`.

## Step 2 - Design the test and get approval

From the conversation and the diff (`git diff main...`), work out what can
only be verified live and turn it into checks, each with a concrete
expected value (row counts, table properties, flow names, job task status,
error message). Think about the lifecycle, not just the first run:
incremental update, full refresh, redeploy after a config change, rerun
after failure - whichever the feature touches.

Keep the stack minimal: the fewest nodes/resources that exercise the
behavior, repo data files as sources (see template), serverless compute.
Include the laktory wheel built from the working tree whenever the code
running on Databricks (pipeline notebook, job tasks) needs the unreleased
change; skip it for pure resource/IaC changes.

Present to the user: slug, resources, checks with expected values. Wait
for approval before creating anything.

## Step 3 - Scaffold

Copy `.claude/skills/e2e-test/template/` to `scratch/e2e/<slug>/`, rename
`pipeline.yaml` to `pl-e2e-<slug>.yaml`, and fill every `{placeholder}`.
For jobs, replace the pipeline with a `jobs:` entry (serverless environment
with the wheel as dependency). Write the checks into the README table
before deploying - the README is the test's record.

Validate before touching the workspace:

```sh
uv run laktory validate --env dev
uv run laktory init --env dev
uv run laktory preview --env dev
```

Check the preview: every workspace path is under `/Users/{you}/.laktory/e2e-<slug>/dev/`
and every resource name carries the slug.

## Step 4 - Deploy

The user approved the plan, so deploy without the interactive prompt:

```sh
uv run laktory deploy --env dev --yes
```

On failure, fix the stack (or, if it's a laktory bug, tell the user - that
may be the finding) and redeploy. Never edit laktory source to make the e2e
pass without saying so explicitly.

## Step 5 - Run and check

```sh
uv run laktory run --env dev --databricks-pipeline pl-e2e-<slug>   # or --databricks-job
uv run laktory run --env dev --databricks-pipeline pl-e2e-<slug> --full-refresh
```

Run SQL checks through the statement API and read `result.data_array`:

```sh
databricks api post /api/2.0/sql/statements -p laktory-dev-cli --json '{
  "warehouse_id": "<id>", "wait_timeout": "50s",
  "statement": "SELECT feed, COUNT(*) FROM laktory.unit_tests.e2e_<prefix>_slv GROUP BY feed"
}'
```

For pipeline internals (flows, expectations, errors) use
`databricks pipelines list-pipeline-events <pipeline_id> -p laktory-dev-cli`;
for jobs, `databricks jobs get-run <run_id> -p laktory-dev-cli`. Get ids with
`databricks pipelines list-pipelines -p laktory-dev-cli --filter "name LIKE 'pl-e2e-<slug>'"`.

To add data mid-test (incremental checks), copy repo files to a new name
under the slug's DBFS folder with `databricks fs cp ... -p laktory-dev-cli`.

Some things are only visible in the UI (pipeline graph, data quality tab).
If a check needs that, give the user the workspace URL and ask them to look
rather than marking it passed.

## Step 6 - Report

Fill the README's Result column and Findings section: pass/fail per check
with the observed value, anything unexpected, follow-ups (bugs, docs,
CHANGELOG). Summarize to the user in chat, failures first. If a check
failed because of a laktory bug, propose the fix; the stack stays up so the
fix can be re-verified by redeploying.

## Step 7 - Teardown (after the user says done)

The user may want to inspect the workspace, so ask before destroying. Then:

```sh
uv run laktory destroy --env dev --yes
databricks fs rm -r dbfs:/laktory/e2e/<slug>/ -p laktory-dev-cli
```

Check for leftovers and drop them (only names with the slug/prefix):

```sql
SHOW TABLES IN laktory.unit_tests LIKE 'e2e_<prefix>_*';
-- DROP TABLE IF EXISTS laktory.unit_tests.e2e_<prefix>_... for each
```

and `databricks workspace list /Users/<you>/.laktory/e2e-<slug> -p laktory-dev-cli`
(delete with `databricks workspace delete --recursive` if it remains).

Keep `scratch/e2e/<slug>/` (it's git-ignored and holds the findings);
remove `.terraform/`, `terraform.tfstate*` and `stack.tf.json` from it. Mark
the README "torn down on <date>".
