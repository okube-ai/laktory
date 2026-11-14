"""List the lines of a unified diff that accept an inline PR review comment.

GitHub only accepts inline comments on lines inside a diff hunk (added or
context lines, `side: RIGHT`); a single comment outside a hunk makes the whole
review fail with 422. Usage:

    gh pr diff <number> | python diff_lines.py            # {path: [[start, end], ...]}
    gh pr diff <number> | python diff_lines.py path line  # exit 0 if commentable
"""

import json
import re
import sys

HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def commentable_lines(diff: str) -> dict[str, list[int]]:
    lines: dict[str, list[int]] = {}
    path = None
    new_line = None
    for row in diff.splitlines():
        if row.startswith("diff --git"):
            path, new_line = None, None
        elif row.startswith("+++ "):
            target = row[4:]
            path = None if target == "/dev/null" else target.removeprefix("b/")
        elif m := HUNK.match(row):
            new_line = int(m.group(1))
        elif path is None or new_line is None:
            continue
        elif row.startswith("+") or row.startswith(" "):
            lines.setdefault(path, []).append(new_line)
            new_line += 1
        # "-" rows (removed) and "\ No newline" rows don't advance the new file
    return lines


def ranges(numbers: list[int]) -> list[list[int]]:
    out = []
    for n in numbers:
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return out


if __name__ == "__main__":
    lines = commentable_lines(sys.stdin.read())
    if len(sys.argv) == 3:
        sys.exit(0 if int(sys.argv[2]) in lines.get(sys.argv[1], []) else 1)
    print(json.dumps({p: ranges(n) for p, n in lines.items()}, indent=1))
