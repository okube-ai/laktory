"""Tests for laktory/cli/_setup_agent.py and the setup-agent CLI command."""

import json

from typer.testing import CliRunner

from laktory.cli import app
from laktory.cli._setup_agent import _GITHUB_INSTRUCTIONS_PATH
from laktory.cli._setup_agent import _LAKTORY_AGENTS_FILENAME
from laktory.cli._setup_agent import _LAKTORY_MARKER
from laktory.cli._setup_agent import _MCP_ENTRY
from laktory.cli._setup_agent import write_agents_md_copilot
from laktory.cli._setup_agent import write_claude_instructions
from laktory.cli._setup_agent import write_claude_md
from laktory.cli._setup_agent import write_github_instructions
from laktory.cli._setup_agent import write_mcp_json
from laktory.cli._setup_agent import write_other_instructions

runner = CliRunner()


# --------------------------------------------------------------------------- #
# write_claude_instructions                                                    #
# --------------------------------------------------------------------------- #


def test_write_claude_instructions_creates(tmp_path):
    write_claude_instructions(tmp_path)
    dst = tmp_path / ".claude" / "docs" / "laktory.md"
    assert dst.exists()
    assert "Laktory" in dst.read_text()


def test_write_claude_instructions_creates_dir(tmp_path):
    write_claude_instructions(tmp_path)
    assert (tmp_path / ".claude" / "docs").is_dir()


def test_write_claude_instructions_always_overwrites(tmp_path):
    write_claude_instructions(tmp_path)
    (tmp_path / ".claude" / "docs" / "laktory.md").write_text("# Outdated\n")
    write_claude_instructions(tmp_path)
    assert "Outdated" not in (tmp_path / ".claude" / "docs" / "laktory.md").read_text()


# --------------------------------------------------------------------------- #
# write_claude_md                                                              #
# --------------------------------------------------------------------------- #


def test_write_claude_md_appends_when_exists(tmp_path):
    original = "# My project\n\nProject instructions.\n"
    (tmp_path / "CLAUDE.md").write_text(original)
    write_claude_md(tmp_path)
    content = (tmp_path / "CLAUDE.md").read_text()
    assert content.startswith(original)
    assert _LAKTORY_MARKER in content
    assert "@.claude/docs/laktory.md" in content


def test_write_claude_md_creates_when_absent(tmp_path):
    write_claude_md(tmp_path)
    assert (tmp_path / "CLAUDE.md").exists()
    content = (tmp_path / "CLAUDE.md").read_text()
    assert _LAKTORY_MARKER in content
    assert "@.claude/docs/laktory.md" in content


def test_write_claude_md_idempotent(tmp_path):
    (tmp_path / "CLAUDE.md").write_text("# My project\n")
    write_claude_md(tmp_path)
    after_first = (tmp_path / "CLAUDE.md").read_text()
    write_claude_md(tmp_path)
    assert (tmp_path / "CLAUDE.md").read_text() == after_first


# --------------------------------------------------------------------------- #
# write_github_instructions                                                    #
# --------------------------------------------------------------------------- #


def test_write_github_instructions_creates(tmp_path):
    write_github_instructions(tmp_path)
    dst = tmp_path / ".github" / "instructions" / "laktory.md"
    assert dst.exists()
    content = dst.read_text()
    assert "applyTo" in content
    assert "Laktory" in content


def test_write_github_instructions_creates_dirs(tmp_path):
    write_github_instructions(tmp_path)
    assert (tmp_path / ".github" / "instructions").is_dir()


def test_write_github_instructions_always_overwrites(tmp_path):
    write_github_instructions(tmp_path)
    dst = tmp_path / ".github" / "instructions" / "laktory.md"
    dst.write_text("# Outdated\n")
    write_github_instructions(tmp_path)
    assert "Outdated" not in dst.read_text()
    assert "Laktory" in dst.read_text()


# --------------------------------------------------------------------------- #
# write_agents_md_copilot                                                      #
# --------------------------------------------------------------------------- #


def test_write_agents_md_copilot_creates(tmp_path):
    write_agents_md_copilot(tmp_path)
    content = (tmp_path / "AGENTS.md").read_text()
    assert _LAKTORY_MARKER in content
    assert _GITHUB_INSTRUCTIONS_PATH in content


def test_write_agents_md_copilot_appends_when_exists(tmp_path):
    original = "# My project\n"
    (tmp_path / "AGENTS.md").write_text(original)
    write_agents_md_copilot(tmp_path)
    content = (tmp_path / "AGENTS.md").read_text()
    assert content.startswith(original)
    assert _GITHUB_INSTRUCTIONS_PATH in content


def test_write_agents_md_copilot_idempotent(tmp_path):
    write_agents_md_copilot(tmp_path)
    first = (tmp_path / "AGENTS.md").read_text()
    write_agents_md_copilot(tmp_path)
    assert (tmp_path / "AGENTS.md").read_text() == first


# --------------------------------------------------------------------------- #
# write_other_instructions                                                     #
# --------------------------------------------------------------------------- #


def test_write_other_instructions_creates(tmp_path):
    write_other_instructions(tmp_path)
    assert (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()
    assert "Laktory" in (tmp_path / _LAKTORY_AGENTS_FILENAME).read_text()
    assert (tmp_path / "AGENTS.md").exists()


def test_write_other_instructions_agents_md_has_marker(tmp_path):
    write_other_instructions(tmp_path)
    assert _LAKTORY_MARKER in (tmp_path / "AGENTS.md").read_text()
    assert _LAKTORY_AGENTS_FILENAME in (tmp_path / "AGENTS.md").read_text()


def test_write_other_instructions_appends_when_exists(tmp_path):
    original = "# My project\n\nProject-specific context.\n"
    (tmp_path / "AGENTS.md").write_text(original)
    write_other_instructions(tmp_path)
    content = (tmp_path / "AGENTS.md").read_text()
    assert content.startswith(original)
    assert _LAKTORY_MARKER in content


def test_write_other_instructions_agents_md_idempotent(tmp_path):
    write_other_instructions(tmp_path)
    first = (tmp_path / "AGENTS.md").read_text()
    write_other_instructions(tmp_path)
    assert (tmp_path / "AGENTS.md").read_text() == first


def test_write_other_instructions_laktory_file_always_updated(tmp_path):
    write_other_instructions(tmp_path)
    (tmp_path / _LAKTORY_AGENTS_FILENAME).write_text("# Outdated content\n")
    write_other_instructions(tmp_path)
    content = (tmp_path / _LAKTORY_AGENTS_FILENAME).read_text()
    assert "Outdated content" not in content
    assert "Laktory" in content


# --------------------------------------------------------------------------- #
# write_mcp_json                                                               #
# --------------------------------------------------------------------------- #


def test_write_mcp_json_creates(tmp_path):
    write_mcp_json(tmp_path)
    mcp = tmp_path / ".mcp.json"
    assert mcp.exists()
    assert json.loads(mcp.read_text())["mcpServers"]["laktory"] == _MCP_ENTRY


def test_write_mcp_json_merges_existing_servers(tmp_path):
    existing = {"mcpServers": {"other": {"command": "npx", "args": ["other"]}}}
    (tmp_path / ".mcp.json").write_text(json.dumps(existing))
    write_mcp_json(tmp_path)
    data = json.loads((tmp_path / ".mcp.json").read_text())
    assert data["mcpServers"]["other"] == existing["mcpServers"]["other"]
    assert data["mcpServers"]["laktory"] == _MCP_ENTRY


def test_write_mcp_json_idempotent(tmp_path):
    write_mcp_json(tmp_path)
    first = (tmp_path / ".mcp.json").read_text()
    write_mcp_json(tmp_path)
    assert (tmp_path / ".mcp.json").read_text() == first


def test_write_mcp_json_invalid_json_skips(tmp_path):
    (tmp_path / ".mcp.json").write_text("{not valid json")
    write_mcp_json(tmp_path)
    assert (tmp_path / ".mcp.json").read_text() == "{not valid json"


# --------------------------------------------------------------------------- #
# setup-agent CLI — agent claude                                               #
# --------------------------------------------------------------------------- #


def test_setup_agent_claude(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["setup-agent", "--agent", "claude"])
    assert result.exit_code == 0
    assert (tmp_path / ".claude" / "docs" / "laktory.md").exists()
    assert "@.claude/docs/laktory.md" in (tmp_path / "CLAUDE.md").read_text()
    assert (tmp_path / ".mcp.json").exists()
    assert not (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()


def test_setup_agent_claude_creates_claude_md(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["setup-agent", "--agent", "claude"])
    assert (tmp_path / "CLAUDE.md").exists()


def test_setup_agent_claude_idempotent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["setup-agent", "--agent", "claude"])
    first_claude = (tmp_path / "CLAUDE.md").read_text()
    runner.invoke(app, ["setup-agent", "--agent", "claude"])
    assert (tmp_path / "CLAUDE.md").read_text() == first_claude


# --------------------------------------------------------------------------- #
# setup-agent CLI — agent copilot                                              #
# --------------------------------------------------------------------------- #


def test_setup_agent_copilot(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["setup-agent", "--agent", "copilot"])
    assert result.exit_code == 0
    dst = tmp_path / ".github" / "instructions" / "laktory.md"
    assert dst.exists()
    assert "applyTo" in dst.read_text()
    assert _GITHUB_INSTRUCTIONS_PATH in (tmp_path / "AGENTS.md").read_text()
    assert (tmp_path / ".mcp.json").exists()
    assert not (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()


# --------------------------------------------------------------------------- #
# setup-agent CLI — agent other                                                #
# --------------------------------------------------------------------------- #


def test_setup_agent_other(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["setup-agent", "--agent", "other"])
    assert result.exit_code == 0
    assert (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()
    assert (tmp_path / "AGENTS.md").exists()
    assert (tmp_path / ".mcp.json").exists()


def test_setup_agent_other_idempotent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["setup-agent", "--agent", "other"])
    first_agents = (tmp_path / "AGENTS.md").read_text()
    first_mcp = (tmp_path / ".mcp.json").read_text()
    runner.invoke(app, ["setup-agent", "--agent", "other"])
    assert (tmp_path / "AGENTS.md").read_text() == first_agents
    assert (tmp_path / ".mcp.json").read_text() == first_mcp


def test_setup_agent_merges_mcp(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    existing = {"mcpServers": {"other": {"command": "npx", "args": ["other"]}}}
    (tmp_path / ".mcp.json").write_text(json.dumps(existing))
    runner.invoke(app, ["setup-agent", "--agent", "other"])
    data = json.loads((tmp_path / ".mcp.json").read_text())
    assert "other" in data["mcpServers"]
    assert "laktory" in data["mcpServers"]
