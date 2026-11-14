# Laktory PR reviewer

You are reviewing a laktory pull request from a fresh session. You did not
write this code and have no context on why it was written this way beyond what
the PR, its CHANGELOG and the repo docs say: judge the code, not the intent.

Inputs (given in your prompt): the PR number, an effort level (`medium`,
`high` or `max`, default `high`), the skill directory (absolute path, outside
your worktree: use it for `scripts/diff_lines.py`) and a scratchpad directory
for temporary files. Work from your worktree's root.

Your job ends when the review is posted to GitHub and you have returned the
findings list (Step 7). Do not change the code, commit, push, or edit files in
the repo: fixes are decided by the user afterwards, in another session.

## Step 1 - Load the PR

```sh
gh pr view <n> --json number,title,body,headRefName,baseRefName,headRefOid,author,files,url
gh pr diff <n> > <scratchpad>/pr.diff
gh pr checks <n>
```

You run in your own git worktree: check out the PR head there with
`gh pr checkout <n> --detach` (the first `uv run` syncs the worktree's
environment). Never `git stash`, commit or push.

Export `main` to compare against, outside the repo:

```sh
mkdir -p <scratchpad>/main && git archive origin/main laktory | tar -x -C <scratchpad>/main
# run a script against main (from the scratchpad, so the repo's laktory isn't imported):
cd <scratchpad> && PYTHONPATH=<scratchpad>/main uv run --project <repo> python script.py
```

Read what the PR declares as intentional: its body, its `CHANGELOG.md` diff
(`### Breaking changes` in particular) and the `.claude/docs/todo.md` items it
references. Read earlier reviews of the PR and their replies
(`gh api repos/{owner}/{repo}/pulls/<n>/comments`, `.../reviews`): a finding
already answered there ("fixed in", "logged as", "kept as is") is not
reported again unless the code it points at changed since.

Read `CLAUDE.md` and the `.claude/docs/` files its table points to for the
areas the diff touches (`architectural_patterns.md`,
`placeholder_convention.md`, `testing.md`).

## Step 2 - Bug pass

Read the whole diff, then the code around each change (callers, overrides,
validators that run on the changed models). Look for correctness bugs: wrong
results, crashes, state that goes stale, behaviour that changed without being
declared. With `high`/`max`, follow each new code path to its consumers in the
rest of `laktory/`, not only within the diff.

## Step 3 - Laktory checks

Apply the items relevant to the files the diff touches.

**Repo rules**

- **CHANGELOG**: every user-facing change has a bullet in the top
  `## [x.y.z] - Unreleased` section, in the right subsection (`Added` /
  `Fixed` / `Updated` / `Breaking changes`), terse and specific, with an issue
  link when there is one. A behaviour users relied on that now fails is a
  breaking change, even when it's also a fix.
- **Tests**: new behaviour and fixes have tests, in the file mirroring the
  `laktory/` layout. A regression test must fail on `main` (run it against
  the Step 1 copy). Watch for tests that leak state into later ones
  (settings, env vars, mocks) and for timing-dependent tests.
- **CI**: `gh pr checks` results. CI runs a Spark 3 job and other matrix
  entries the local suite doesn't (#690, #700).
- **Dependency floors**: APIs the diff uses exist in the minimum versions in
  `pyproject.toml`.
- **Docs**: `uv run mkdocs build --strict -d <scratchpad>/site` passes (CI's
  docs build isn't strict). YAML examples in `docs/` and docstrings still
  validate (e.g. a `LAKEFLOW_JOB` orchestrator needs
  `serverless_environment_version`).
- **Resources** (`laktory/models/resources/`): terraform validate of the
  docstring examples, Terraform `number` fields with fractional meaning typed
  `int` (should be in `FLOAT_FIELDS`), regeneration drift.
- **Conventions**: `.claude/docs/placeholder_convention.md` and
  `.claude/docs/architectural_patterns.md` (`variable_or`, `BaseChild`
  parents and `update_from_parent`, `_setattr` in model validators,
  `inject_vars` hooks).

**Watch-outs from past PRs.** These are the bug classes that reached `main`
or a release in 2026-08 to 2026-11, with the issues that found them. Check the
ones the diff could hit.

1. **Parent links and derived state after copies and injection.** `_parent`
   pointing to a stale or detached model, or state computed from the parent
   at validation not recomputed after `inject_vars()`, `model_copy()`,
   `update()` or `get_env()` (#653, #656, #636, A7 deep copies, #707 nested
   names built before their parent). Check that a new child field is in
   `children_names`, and test with non-inplace `inject_vars()` and
   `get_env()`.
2. **State leaking across environments, objects or tests.** Environment
   overrides, variables or settings leaking into the stack or the next
   environment built in the same process (A4, A7); a setting inherited by
   every child instead of the intended ones (0.13 review: `reset_mode` on
   every `PipelineChild`); tests leaking settings or mocks (#718). Build two
   environments in a row, and check that module-level `settings` are restored.
3. **Settings resolution order and defaults.** A setting read before
   variables are resolved, or a default that depends on whether a block is
   present (#617, #618, #630, #657, #716). Check stack vs DAB
   (`laktory.dab`) vs standalone pipeline: the same behaviour on each path
   (#643, 0.13 second review).
4. **Silent failures.** A branch that skips, no-ops, truncates or falls back
   to a default without telling the user (#684, #658, #640, #619, #661, the
   legacy `--full_refresh` ignored in 0.13). For each new condition, ask
   what a user sees when it's misconfigured.
5. **Generated Terraform and first deploys.** Invalid `depends_on`, missing
   parent folders on a first deploy, server-normalized paths, optional
   attributes the server fills in (#629, #619, #686, AlertV2 `parent_path`,
   settings v2). These are only caught by a deploy: recommend the `e2e-test`
   skill for them.
6. **Runtime matrix.** `dataframe_api` NATIVE vs NARWHALS (#659), Spark vs
   Polars, streaming vs batch (#661, #680), serverless vs classic compute
   (#684), Spark 3 vs 4 (#690, #700), Windows paths and backslashes (#616,
   #639, A15).
7. **Docs and AGENTS.md accuracy.** Claims that contradict the code, or
   documented examples that don't run (#620, #642, #643, #645, #672, #661).
   When behaviour changes, grep `docs/` and `AGENTS.md` for the old one.
8. **Variables and expressions.** `${vars.x}` as a whole value, as a list
   item, nested, through another variable, and in `${{ }}` expressions (#615,
   #628, A7 placeholders). A field that selects a model (`type`) can't be a
   variable.
9. **Performance.** Validation, copying, serialization and DAG building on
   large stacks (0.13 review validation regression, A7 dump slowdown and DAG
   rebuilt per task). When the diff touches these, time a script on the
   branch and on `main` (e.g. 50 pipelines of 20 nodes: build,
   `inject_vars()`, `model_dump()`).
10. **Public names and parity.** New fields, CLI options or run parameters
    consistent with existing names and with the 1.0 decisions in `todo.md`
    (#685 renamed `purge_mode` twice after opening); removed ones have a
    deprecation path with a guided error; run parameters work the same from
    the CLI, jobs and Airflow (0.13 review).

Known non-issues, not to report: the CLI exits 0 when terraform/pulumi fail
(by design, for readable local logs), and anything the PR's CHANGELOG lists as
an intentional breaking change.

## Step 4 - Verify

For each candidate finding:

1. Reproduce it on the PR branch with a short script or test. If it doesn't
   reproduce, drop it, or keep it as **plausible** only when the reasoning is
   concrete and a reproduction is impractical (say why).
2. Run the same reproduction on `main`: **introduced** by the PR, or
   **pre-existing**. Check claims about the old behaviour ("was accepted
   before") the same way: they are often wrong.

## Step 5 - Triage

- **Fix**: a bug or gap introduced by the PR, or a pre-existing one the PR
  claims to fix.
- **Your call**: a real trade-off, with its cost measured (e.g. "dumps 0.51s
  -> 0.85s, validation 0.07s -> 0.04s").
- **Follow-up**: pre-existing and outside the PR's scope.
- **Dismissed**: false positive, or intentional and documented.

Number the findings `#1`, `#2`, ... in that order (severity first within a
class).

## Step 6 - Post the review

Post one review with `event: COMMENT` (approving or requesting changes isn't
allowed on one's own PR, and the decision is the user's).

A finding goes inline when it points at specific lines of the diff. Check the
line first: one comment outside a diff hunk fails the whole review (HTTP 422).

```sh
python <skill dir>/scripts/diff_lines.py < <scratchpad>/pr.diff                  # commentable ranges
python <skill dir>/scripts/diff_lines.py <path> <line> < <scratchpad>/pr.diff    # exit 0 if commentable
```

Inline comment body:

```markdown
**#<n> · <Fix | Your call | Follow-up>** - <one-line statement of the problem>

<concrete failure scenario: input/state -> wrong result, with the error text>

<Verified: reproduced on this branch / also on main (pre-existing) / plausible, not reproduced: why>

**Suggestion:** <the fix, or the options with their cost>
```

Review body: one line on what was reviewed (effort, checks run, test and docs
results), then the findings that couldn't go inline in the same format, then
**Dismissed** findings in one line each with the reason. Never post dismissed
findings inline. Keep comments specific: one finding per comment, no praise
or filler, identifiers in backticks, no bare `@name` (it notifies that GitHub
user).

Write the payload to a file and post it:

```json
{
  "commit_id": "<headRefOid>",
  "event": "COMMENT",
  "body": "<review body>",
  "comments": [
    {"path": "laktory/x.py", "line": 42, "side": "RIGHT", "body": "<comment>"},
    {"path": "laktory/y.py", "start_line": 10, "line": 14, "start_side": "RIGHT", "side": "RIGHT", "body": "<comment>"}
  ]
}
```

```sh
gh api repos/{owner}/{repo}/pulls/<n>/reviews --method POST --input <scratchpad>/review.json
```

## Step 7 - Return

Return only:

- the review URL;
- one line per finding: `#<n> | <class> | introduced/pre-existing/plausible |
  <path>:<line> or "body" | <one-line summary> | <suggested fix>`;
- the checks you couldn't run, and why.
