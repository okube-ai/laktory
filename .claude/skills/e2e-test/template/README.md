# e2e - {title} ({issue ref})

Checks, on a real Databricks workspace, what CI can't: {why this needs a live workspace}.

- Branch / commit: `{branch}` @ `{short sha}`
- Workspace: `laktory-dev-cli` profile
- Tables: `laktory.unit_tests.e2e_{prefix}_*`
- DBFS data: `dbfs:/laktory/e2e/{slug}/`
- Workspace files: `/Users/{you}/.laktory/e2e-{slug}/dev/`

The stack deploys:

- {resource}: {why}

## Run

From this folder:

```sh
uv run laktory deploy --env dev --yes
uv run laktory run --env dev --databricks-pipeline pl-e2e-{slug}
```

## Checks

| # | Check | Expected | Result |
|---|-------|----------|--------|
| 1 | {what} | {expected value} | pending |

Details (SQL, commands) for each check:

**1. {what}**

```sql
SELECT ...;
-- expected: ...
```

## Findings

{Filled in after the run: pass/fail per check, surprises, follow-ups.}

## Teardown

```sh
uv run laktory destroy --env dev --yes
databricks fs rm -r dbfs:/laktory/e2e/{slug}/ -p laktory-dev-cli
```

Then drop any `laktory.unit_tests.e2e_{prefix}_*` table that survived the pipeline deletion.
