from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated
from typing import Optional

import typer
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

from laktory._logger import get_logger
from laktory.cli._common import AgentValidator
from laktory.cli.app import app
from laktory.constants import AGENT_CHOICES

logger = get_logger(__name__)


_LAKTORY_MARKER = "<!-- laktory -->"
_LAKTORY_AGENTS_FILENAME = "AGENTS_LAKTORY.md"
_GITHUB_INSTRUCTIONS_PATH = ".github/instructions/laktory.md"

_MCP_ENTRY = {
    "command": "python",
    "args": ["-m", "laktory.mcp.server"],
}

# Reference appended to AGENTS.md for the "other" path (Markdown link)
_AGENTS_REFERENCE = (
    f"\n---\n{_LAKTORY_MARKER}\n\n"
    f"For Laktory model docs, YAML patterns, and examples, "
    f"see [{_LAKTORY_AGENTS_FILENAME}]({_LAKTORY_AGENTS_FILENAME}).\n"
)

# Reference appended to AGENTS.md for the "copilot" path (Markdown link)
_COPILOT_AGENTS_REFERENCE = (
    f"\n---\n{_LAKTORY_MARKER}\n\n"
    f"For Laktory model docs, YAML patterns, and examples, "
    f"see [{_GITHUB_INSTRUCTIONS_PATH}]({_GITHUB_INSTRUCTIONS_PATH}).\n"
)

# Reference appended to CLAUDE.md (@import syntax read by Claude Code)
_CLAUDE_REFERENCE = f"\n---\n{_LAKTORY_MARKER}\n\n@.claude/docs/laktory.md\n"

# Minimal CLAUDE.md created when the user selects "claude" but has none yet
_CLAUDE_MINIMAL = _CLAUDE_REFERENCE.lstrip()

# Frontmatter that makes GitHub Copilot auto-apply the instructions file
_GITHUB_FRONTMATTER = "---\napplyTo: '**'\n---\n\n"


def _read_laktory_source() -> str | None:
    src = Path(__file__).parent.parent / "AGENTS.md"
    if not src.exists():
        # logger.info("AGENTS.md source not found in installed package — skipping")
        return None
    return src.read_text(encoding="utf-8")


def _append_to_agents_md(target_dir: Path, reference: str) -> None:
    """Idempotently append a reference block to AGENTS.md."""
    agents_dst = target_dir / "AGENTS.md"
    if agents_dst.exists():
        existing = agents_dst.read_text(encoding="utf-8")
        if _LAKTORY_MARKER in existing:
            # logger.info("AGENTS.md: Laktory reference already present — skipping")
            return
        logger.info(f"Adding laktory agent instructions reference to {agents_dst}...")
        agents_dst.write_text(existing + reference, encoding="utf-8")
    else:
        logger.info(f"Creating laktory agent instructions reference at {agents_dst}...")
        agents_dst.write_text(reference.lstrip(), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Claude Code                                                                  #
# --------------------------------------------------------------------------- #


def write_claude_instructions(target_dir: str | Path = ".") -> None:
    """Write .claude/laktory.md with the full Laktory instructions."""
    content = _read_laktory_source()
    if content is None:
        return
    claude_dir = Path(target_dir) / ".claude" / "docs"
    claude_dir.mkdir(parents=True, exist_ok=True)
    dst = claude_dir / "laktory.md"
    logger.info(f"Writing laktory agent instructions to {dst}...")
    dst.write_text(content, encoding="utf-8")


def write_claude_md(target_dir: str | Path = ".") -> None:
    """Append @.claude/laktory.md import to CLAUDE.md; create it if absent."""
    dst = Path(target_dir) / "CLAUDE.md"
    if dst.exists():
        existing = dst.read_text(encoding="utf-8")
        if _LAKTORY_MARKER in existing:
            # logger.info("CLAUDE.md: Laktory reference already present — skipping")
            return
        logger.info(f"Adding laktory agent instructions reference to {dst}...")
        dst.write_text(existing + _CLAUDE_REFERENCE, encoding="utf-8")
    else:
        logger.info(f"Creating laktory agent instructions reference at {dst}...")
        dst.write_text(_CLAUDE_MINIMAL, encoding="utf-8")


# --------------------------------------------------------------------------- #
# GitHub Copilot                                                               #
# --------------------------------------------------------------------------- #


def write_github_instructions(target_dir: str | Path = ".") -> None:
    """Write .github/instructions/laktory.md with applyTo frontmatter."""
    content = _read_laktory_source()
    if content is None:
        return
    instructions_dir = Path(target_dir) / ".github" / "instructions"
    instructions_dir.mkdir(parents=True, exist_ok=True)
    dst = instructions_dir / "laktory.md"
    logger.info(f"Writing laktory agent instructions to {dst}...")
    dst.write_text(_GITHUB_FRONTMATTER + content, encoding="utf-8")


def write_agents_md_copilot(target_dir: str | Path = ".") -> None:
    """Idempotently add a link to .github/instructions/laktory.md in AGENTS.md."""
    _append_to_agents_md(Path(target_dir), _COPILOT_AGENTS_REFERENCE)


# --------------------------------------------------------------------------- #
# Generic / other agents                                                       #
# --------------------------------------------------------------------------- #


def write_other_instructions(target_dir: str | Path = ".") -> None:
    """Write AGENTS_LAKTORY.md and add a reference to it in AGENTS.md."""
    content = _read_laktory_source()
    if content is None:
        return
    laktory_dst = Path(target_dir) / _LAKTORY_AGENTS_FILENAME
    laktory_dst.write_text(content, encoding="utf-8")
    logger.info(f"Writing laktory agent instructions to {laktory_dst}...")
    _append_to_agents_md(Path(target_dir), _AGENTS_REFERENCE)


# Backward-compat alias
write_agents_md = write_other_instructions


# --------------------------------------------------------------------------- #
# MCP                                                                          #
# --------------------------------------------------------------------------- #


def write_mcp_json(target_dir: str | Path = ".") -> None:
    dst = Path(target_dir) / ".mcp.json"

    if dst.exists():
        logger.info(f"Updating {dst} with laktory configuration ...")
        try:
            data = json.loads(dst.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.info("Could not parse existing file — skipping")
            return
        servers = data.setdefault("mcpServers", {})
        if servers.get("laktory") == _MCP_ENTRY:
            logger.info("laktory server already configured — skipping")
            return
        servers["laktory"] = _MCP_ENTRY
        dst.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    else:
        data = {"mcpServers": {"laktory": _MCP_ENTRY}}
        logger.info(f"Writing {dst} with laktory configuration ...")
        dst.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
# CLI command                                                                  #
# --------------------------------------------------------------------------- #


@app.command()
def setup_agent(
    agent: Annotated[
        Optional[str],
        typer.Option(
            "--agent",
            "-a",
            help=f"Agent framework {AGENT_CHOICES}",
        ),
    ] = None,
):
    """
    Set up AI coding-agent support in the current directory.

    Writes agent instruction files and .mcp.json (MCP server configuration).
    Safe to run on existing projects — all writes are idempotent.

    Use --agent to skip the interactive prompt:

    - claude   → .claude/laktory.md + @import in CLAUDE.md
    - copilot  → .github/instructions/laktory.md + reference in AGENTS.md
    - other    → AGENTS_LAKTORY.md + reference in AGENTS.md

    Examples
    --------
    ```cmd
    laktory setup-agent
    laktory setup-agent --agent claude
    ```

    References
    ----------
    * [MCP](https://modelcontextprotocol.io/)
    """
    if agent is None:
        agent = prompt(
            f"Select agent {AGENT_CHOICES}: ",
            completer=WordCompleter(AGENT_CHOICES, ignore_case=True),
            validator=AgentValidator(),
        )

    agent = agent.lower()

    if agent == "claude":
        write_claude_instructions()
        write_claude_md()
    elif agent == "copilot":
        write_github_instructions()
        write_agents_md_copilot()
    else:
        write_other_instructions()

    write_mcp_json()
