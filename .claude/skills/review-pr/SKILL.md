---
name: review-pr
description: This skill should be used when the user asks to "review PR 123", "review this PR", "review my branch before merge", "run a final review", or otherwise wants a laktory pull request reviewed. A fresh, unbiased reviewer agent finds bugs and checks laktory-specific risks (CHANGELOG, breaking changes, tests, docs, dependency floors, performance, and the bug classes past PRs missed), verifies each finding on the branch and on main, and posts them to the GitHub PR as inline review comments; the user then decides what to fix or log, and the threads are answered and resolved.
---

Review a laktory pull request without the bias of the session that wrote it:
a fresh agent reviews the PR and posts the findings to GitHub, then this
session relays them, waits for the user's decisions, applies them, and closes
the loop on GitHub.

The main target is the user's own branches before merge; contributor PRs
(e.g. from the `contributor` remote) work the same way, without pushing to
their branch. Run this inline in the main conversation: Step 4 waits for the
user's decisions turn by turn.

## Step 1 - Find the PR

- A PR number: use it.
- No number: the current branch's PR (`gh pr view --json number,url`). If
  there is none, ask whether to push and open a draft PR (the review is posted
  to a PR) and stop until answered.

The reviewer reviews what is on GitHub: if the branch has local commits or
changes that aren't pushed (`git status -sb`), tell the user they won't be
reviewed, and ask whether to push first.

## Step 2 - Launch a fresh reviewer

Launch an agent with the Agent tool:

- `subagent_type: "general-purpose"`, never `"fork"` (a fork inherits this
  conversation, which is the bias to avoid);
- `isolation: "worktree"`, so it checks out the PR without touching the
  user's working tree;
- this prompt, filled in and nothing else:

```text
Review laktory pull request #<n> by following <skill dir>/reviewer.md step by
step. Skill directory: <skill dir>. Effort: <medium|high|max, default high>.
Scratchpad directory for temporary files: <scratchpad>/review-<n>.
```

`<skill dir>` is the absolute path of this skill in the main checkout
(`<repo root>/.claude/skills/review-pr`): the PR branch checked out in the
worktree may predate the skill.

Don't add context from this conversation to the prompt: no summary of the
change, no known issues, no decisions already made. The reviewer reads the PR,
its CHANGELOG and earlier review replies on GitHub, which is where intent must
be recorded. Wait for the agent's completion notification; don't predict its
result.

## Step 3 - Relay the review

Read the posted review on GitHub (`gh api repos/{owner}/{repo}/pulls/<n>/reviews`
and `.../comments`) rather than trusting only the agent's summary, and
present it to the user:

- the review link;
- the findings by number, grouped as posted (**Fix**, **Your call**,
  **Follow-up**, with **Dismissed** in one line), one line each;
- a proposal (e.g. "fix #1-#3, log #5 in todo.md, keep #4").

This session may disagree with a finding: say so in the proposal, labelled as
the author session's view, with the reason. Never drop or soften a finding in
the relay; the user decides.

## Step 4 - Wait for decisions

Stop and wait for an explicit decision per finding (or a rule such as "fix
1-3, log 5"). Do not change code, files or git state on silence, an ambiguous
reply, or a question; answer it first.

## Step 5 - Apply the decisions

- **Fix**: implement with tests. Each new test must fail without the fix:
  run it against a `git archive origin/main laktory` copy, or temporarily
  patch the old behaviour back. Update the CHANGELOG bullet the fix belongs
  to, or add one.
- **Log**: add a `## A<n> - <title>` section at the end of
  `.claude/docs/todo.md` (date found, problem, why not fixed now, suggested
  fix) and the item to the `todo.md` row of the `CLAUDE.md` documentation
  table.
- Lint only the changed files: `uv run ruff check --no-fix --select F,I <files>`
  and `uv run ruff format --check <files>` (`ruff check` auto-fixes in this
  repo).
- Run the targeted tests, then the full suite, writing to a log because the
  py4j shutdown noise buries the summary:

```sh
uv run pytest -m "not databricks_connect and not slow" tests -q -p no:cacheprovider \
  --basetemp=<scratchpad>/<fresh-dir> -W "error:Pydantic serializer warnings:UserWarning" \
  > <scratchpad>/pytest.log 2>&1; grep -E "^(FAILED|ERROR)|[0-9]+ passed" <scratchpad>/pytest.log
```

Don't commit or push unless the user asks in this conversation; then stage
only the PR's files (never `git add -A`) and push to the PR branch. For a
contributor PR, don't push to their branch.

## Step 6 - Close the loop on GitHub

Once the fixes are pushed (or the user says so), reply to each inline comment
with its outcome, and resolve the threads of fixed findings:

```sh
# reply: fixed in <sha> / logged as A<n> in todo.md / kept as is: <reason>
gh api repos/{owner}/{repo}/pulls/<n>/comments/<comment_id>/replies --method POST -f body="<outcome>"

# thread ids, then resolve
gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){reviewThreads(first:100){nodes{id isResolved comments(first:1){nodes{databaseId}}}}}}}' -F o=<owner> -F r=<repo> -F n=<n>
gh api graphql -f query='mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{isResolved}}}' -F id=<thread id>
```

Findings from the review body get their outcomes in one PR comment
(`gh pr comment <n>`). These replies are also what the next reviewer reads to
skip decided findings, so state each decision and its reason.

## Step 7 - Re-review (optional)

After substantial fixes, offer another review: repeat from Step 2 with a new
agent (never resume the previous reviewer, it has seen its own conclusions).
