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

HUNK = re.compile(r"^@@ -\d+(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def _target_path(header: str) -> str | None:
    # `+++ b/path`; git appends a tab to paths with spaces and quotes paths
    # with special characters
    target = header[4:].rstrip("\t")
    if target == "/dev/null":
        return None
    if target.startswith('"') and target.endswith('"'):
        target = target[1:-1].encode().decode("unicode_escape")
    return target.removeprefix("b/")


def commentable_lines(diff: str) -> dict[str, list[int]]:
    lines: dict[str, list[int]] = {}
    path = None
    new_line = 0
    old_left = new_left = 0  # lines left in the current hunk
    for row in diff.splitlines():
        if old_left > 0 or new_left > 0:
            # Inside a hunk: rows are content, even when they look like headers
            # (e.g. an added line starting with `++ `)
            if row.startswith("+"):
                lines.setdefault(path, []).append(new_line)
                new_line += 1
                new_left -= 1
            elif row.startswith("-"):
                old_left -= 1
            elif row.startswith(" ") or row == "":
                if path is not None:
                    lines.setdefault(path, []).append(new_line)
                new_line += 1
                old_left -= 1
                new_left -= 1
            # "\ No newline at end of file" doesn't count
        elif row.startswith("diff --git"):
            path = None
        elif row.startswith("+++ "):
            path = _target_path(row)
        elif m := HUNK.match(row):
            old_left = int(m.group(1) or 1)
            new_line = int(m.group(2))
            new_left = int(m.group(3) or 1)
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
    print(json.dumps({p: ranges(n) for p, n in lines.items() if p}, indent=1))
