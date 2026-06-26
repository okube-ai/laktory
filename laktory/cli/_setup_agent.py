from __future__ import annotations

import json
from pathlib import Path

from laktory._logger import get_logger
from laktory.cli.app import app

logger = get_logger(__name__)


_LAKTORY_MARKER = "<!-- laktory -->"
_LAKTORY_AGENTS_FILENAME = "AGENTS_LAKTORY.md"

_MCP_ENTRY = {
    "command": "python",
    "args": ["-m", "laktory.mcp.server"],
}

_AGENTS_REFERENCE = (
    f"\n---\n{_LAKTORY_MARKER}\n\n"
    f"For Laktory model docs, YAML patterns, and examples, "
    f"see [{_LAKTORY_AGENTS_FILENAME}]({_LAKTORY_AGENTS_FILENAME}).\n"
)


def write_agents_md(target_dir: str | Path = ".") -> None:
    src = Path(__file__).parent.parent / "AGENTS.md"

    if not src.exists():
        print("AGENTS.md: source not found in installed package — skipping")
        return

    laktory_content = src.read_text(encoding="utf-8")

    # Always overwrite AGENTS_LAKTORY.md — laktory owns it entirely
    laktory_dst = Path(target_dir) / _LAKTORY_AGENTS_FILENAME
    laktory_dst.write_text(laktory_content, encoding="utf-8")
    print(f"{_LAKTORY_AGENTS_FILENAME}: written to {laktory_dst}")

    # Add one-time reference in AGENTS.md (idempotent via marker)
    agents_dst = Path(target_dir) / "AGENTS.md"
    if agents_dst.exists():
        existing = agents_dst.read_text(encoding="utf-8")
        if _LAKTORY_MARKER in existing:
            print("AGENTS.md: Laktory reference already present — skipping")
            return
        agents_dst.write_text(existing + _AGENTS_REFERENCE, encoding="utf-8")
        print(f"AGENTS.md: Laktory reference appended to {agents_dst}")
    else:
        agents_dst.write_text(_AGENTS_REFERENCE.lstrip(), encoding="utf-8")
        print("AGENTS.md: created with Laktory reference")


def write_mcp_json(target_dir: str | Path = ".") -> None:
    dst = Path(target_dir) / ".mcp.json"

    if dst.exists():
        try:
            data = json.loads(dst.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(".mcp.json: could not parse existing file — skipping")
            return
        servers = data.setdefault("mcpServers", {})
        if servers.get("laktory") == _MCP_ENTRY:
            print(".mcp.json: laktory server already configured — skipping")
            return
        servers["laktory"] = _MCP_ENTRY
        dst.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f".mcp.json: laktory server added to {dst}")
    else:
        data = {"mcpServers": {"laktory": _MCP_ENTRY}}
        dst.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f".mcp.json: written to {dst}")


@app.command()
def setup_agent():
    """
    Set up AI coding-agent support in the current directory.

    Writes AGENTS.md (Laktory model reference for AI agents) and .mcp.json
    (MCP server configuration). Safe to run on existing projects: AGENTS.md
    content is appended once (idempotent), not overwritten; .mcp.json merges
    the laktory server entry without touching other configured servers.

    Examples
    --------
    ```cmd
    laktory setup-agent
    ```

    References
    ----------
    * [MCP](https://modelcontextprotocol.io/)
    """
    write_agents_md()
    write_mcp_json()
