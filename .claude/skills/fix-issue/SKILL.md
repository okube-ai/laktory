---
name: fix-issue
description: This skill should be used when the user asks to "fix issue 123", "work on issue #456", "fix bug 789", or otherwise wants an end-to-end fix for a laktory GitHub issue - fetching the issue, branching, validating and reproducing the bug, planning and proposing a fix for approval, updating the CHANGELOG, implementing with regression tests, and opening a PR.
---

Fix a laktory GitHub issue end-to-end: fetch it, branch, verify it's real,
propose a fix, get approval, update the changelog, implement it with tests,
and open a PR.

Run this inline in the main conversation, not as an isolated background
agent - step 5 requires waiting for the user's explicit go-ahead before any
code or git state changes, and that can only happen turn-by-turn.

## Step 1 - Fetch the issue

Run `gh issue view <number>` from the repo root (the remote is inferred from
the current directory - this skill only makes sense inside the laktory
repo). Capture the title, body, labels, and in particular any "Suggested
fix" or "Prompt for Claude" section - issues in this repo are sometimes
filed with a root-cause analysis already attached, which is useful context
but must still be verified in Step 3, not trusted blindly.

## Step 2 - Create a branch

Run `git status` first. If there are uncommitted changes, stop and ask the
user how to handle them (stash vs. keep working on the current branch)
instead of switching branches over top of them.

Then:

```sh
git fetch origin main
git checkout -b fix/<issue#>-<slug> --no-track origin/main
```

`--no-track` matters: without it, the new branch's upstream is set to
`origin/main`, so a stray plain `git push` before Step 8 would push straight
to `main` on origin instead of creating a new remote branch. Step 8's
`git push -u origin fix/<issue#>-<slug>` sets the correct upstream once the
branch is actually ready to share.

`<slug>` is 1-2 kebab-case words distilled from the issue title, not a
restatement of it - e.g. `frozen-type`, not `explicit-type-crashes-inject-vars`.
Matches the repo's existing convention (`fix/618-workspace-root`,
`fix/615-recursive-vars-solve`).

## Step 3 - Validate the issue is real

Do not treat the issue description as ground truth. Read the code paths it
references, and reproduce the reported behavior - a throwaway script (`uv
run python -c "..."`) or an existing/adapted test - before treating the bug
as confirmed.

If it doesn't reproduce, or is already fixed on `main`, stop here and report
that to the user instead of inventing a fix. Ask whether to close the loop
or dig further - do not proceed to Step 4 on an unconfirmed bug.

## Step 4 - Plan the fix

Identify the true root cause, not just the symptom. Sketch the minimal diff.
Identify where regression tests belong by mirroring the existing `tests/`
layout, which mirrors `laktory/`'s structure (e.g. a fix in
`laktory/models/basemodel.py` gets a test in `tests/test_basemodel.py` or a
more specific existing test file if one already covers that behavior).

Follow the repo's standing conventions: no unrelated refactors, no comments
unless the WHY is genuinely non-obvious, reuse existing patterns instead of
inventing new abstractions, keep the diff as small as the fix requires.

## Step 5 - Explain the fix and ask for approval

Post a concise summary covering:
- The root cause
- The specific file(s)/function(s) to change, and how
- What regression test(s) will be added, and where
- The planned CHANGELOG line

Then stop and wait for an explicit go-ahead ("yes", "approved", "go", etc.).
Do not proceed to Step 6 on silence, an ambiguous reply, or a reply that only
asks a clarifying question - resolve that first.

## Step 6 - Update the CHANGELOG

Read the top `## [x.y.z] - Unreleased` section of `CHANGELOG.md` and find the
matching subsection (`### Added` / `### Fixed` / `### Updated` / `### Breaking
changes`). Replace its `* n/a` placeholder, or append a new bullet if the
section already has real entries.

Format: a one-sentence description of the fix ending with an issue link,
matching the existing terse-but-specific style:

```
* <description> [[#<issue#>](https://github.com/okube-ai/laktory/issues/<issue#>)]
```

## Step 7 - Implement

Make the fix and add the regression test(s) agreed in Step 5. Run the
targeted/affected test files (not the full suite yet) and iterate until
green.

## Step 8 - Commit, push, open PR

Follow the standard git-commit protocol: stage only the specific files that
changed (never `git add -A` / `git add .`), write the commit message via a
heredoc ending with `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`.

```sh
git push -u origin fix/<issue#>-<slug>
gh pr create --title "<short, human-readable title referencing the issue>" \
  --base main \
  --body "$(cat <<'EOF'
Fixes #<issue#>

## Summary
- <1-3 bullets>

## Test plan
- [ ] <targeted tests run in Step 7>
- [ ] <full suite result from Step 9>
EOF
)"
```

The `Fixes #<issue#>` line is a GitHub auto-close keyword (`Closes`/`Resolves` work too) - GitHub
closes the linked issue automatically the moment this PR merges into `main`. Nothing further to
do; don't try to close the issue manually via `gh issue close`, since the skill's own run ends
(Step 9) well before the PR is actually merged.

## Step 9 - Run the full test suite

```sh
uv run pytest -m "not databricks_connect" --cov=laktory tests
```

If anything fails, check whether it also fails on `main` before concluding
it's a real regression (e.g. `git stash`, re-run, `git stash pop`) - some
failures are environment-only (e.g. missing Databricks credentials) and
pre-exist the fix.

If it's a real regression introduced by the fix, patch it and push a
follow-up commit to the already-open PR. Otherwise, report pre-existing /
environment-only failures as such, not as blockers. Report the final
pass/fail result to the user, and update the PR's test-plan checklist.

## Notes

- If the user invokes this skill while another fix is already mid-flight on
  a different branch, don't stack work silently - surface that as part of
  Step 2's `git status` check and ask how to proceed.
