"""Tests for laktory/cli/_agent_files.py and the setup-agent CLI command."""

import json

from typer.testing import CliRunner

from laktory.cli import app
from laktory.cli._setup_agent import _LAKTORY_AGENTS_FILENAME
from laktory.cli._setup_agent import _LAKTORY_MARKER
from laktory.cli._setup_agent import _MCP_ENTRY
from laktory.cli._setup_agent import write_agents_md
from laktory.cli._setup_agent import write_claude_md
from laktory.cli._setup_agent import write_mcp_json

runner = CliRunner()


# --------------------------------------------------------------------------- #
# write_agents_md                                                              #
# --------------------------------------------------------------------------- #


def test_write_agents_md_creates(tmp_path):
    write_agents_md(tmp_path)
    assert (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()
    assert "Laktory" in (tmp_path / _LAKTORY_AGENTS_FILENAME).read_text()
    assert (tmp_path / "AGENTS.md").exists()


def test_write_agents_md_marker_in_agents_not_in_laktory(tmp_path):
    write_agents_md(tmp_path)
    assert _LAKTORY_MARKER in (tmp_path / "AGENTS.md").read_text()
    assert _LAKTORY_MARKER not in (tmp_path / _LAKTORY_AGENTS_FILENAME).read_text()


def test_write_agents_md_reference_in_agents(tmp_path):
    write_agents_md(tmp_path)
    agents_content = (tmp_path / "AGENTS.md").read_text()
    assert _LAKTORY_AGENTS_FILENAME in agents_content


def test_write_agents_md_appends_when_exists(tmp_path):
    existing = "# My project\n\nProject-specific context.\n"
    (tmp_path / "AGENTS.md").write_text(existing)
    write_agents_md(tmp_path)
    content = (tmp_path / "AGENTS.md").read_text()
    assert "My project" in content
    assert _LAKTORY_MARKER in content
    assert _LAKTORY_AGENTS_FILENAME in content


def test_write_agents_md_existing_content_preserved(tmp_path):
    original = "# My project\n\nImportant project context.\n"
    (tmp_path / "AGENTS.md").write_text(original)
    write_agents_md(tmp_path)
    content = (tmp_path / "AGENTS.md").read_text()
    assert content.startswith(original)


def test_write_agents_md_agents_md_idempotent(tmp_path):
    write_agents_md(tmp_path)
    first = (tmp_path / "AGENTS.md").read_text()
    write_agents_md(tmp_path)
    assert (tmp_path / "AGENTS.md").read_text() == first


def test_write_agents_md_laktory_file_always_updated(tmp_path):
    write_agents_md(tmp_path)
    # Simulate an outdated AGENTS_LAKTORY.md
    (tmp_path / _LAKTORY_AGENTS_FILENAME).write_text("# Outdated content\n")
    write_agents_md(tmp_path)
    content = (tmp_path / _LAKTORY_AGENTS_FILENAME).read_text()
    assert "Outdated content" not in content
    assert "Laktory" in content


def test_write_agents_md_append_idempotent(tmp_path):
    (tmp_path / "AGENTS.md").write_text("# My project\n")
    write_agents_md(tmp_path)
    after_first = (tmp_path / "AGENTS.md").read_text()
    write_agents_md(tmp_path)
    assert (tmp_path / "AGENTS.md").read_text() == after_first


# --------------------------------------------------------------------------- #
# write_claude_md                                                              #
# --------------------------------------------------------------------------- #


def test_write_claude_md_skips_when_absent(tmp_path, capsys):
    write_claude_md(tmp_path)
    assert not (tmp_path / "CLAUDE.md").exists()
    assert "skipping" in capsys.readouterr().out


def test_write_claude_md_appends_when_exists(tmp_path):
    original = "# My Laktory project\n\nProject instructions.\n"
    (tmp_path / "CLAUDE.md").write_text(original)
    write_claude_md(tmp_path)
    content = (tmp_path / "CLAUDE.md").read_text()
    assert content.startswith(original)
    assert _LAKTORY_MARKER in content
    assert _LAKTORY_AGENTS_FILENAME in content


def test_write_claude_md_idempotent(tmp_path):
    (tmp_path / "CLAUDE.md").write_text("# My project\n")
    write_claude_md(tmp_path)
    after_first = (tmp_path / "CLAUDE.md").read_text()
    write_claude_md(tmp_path)
    assert (tmp_path / "CLAUDE.md").read_text() == after_first


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
# setup-agent CLI command                                                      #
# --------------------------------------------------------------------------- #


def test_setup_agent_fresh(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["setup-agent"])
    assert result.exit_code == 0
    assert (tmp_path / _LAKTORY_AGENTS_FILENAME).exists()
    assert (tmp_path / "AGENTS.md").exists()
    assert (tmp_path / ".mcp.json").exists()


def test_setup_agent_idempotent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["setup-agent"])
    first_agents = (tmp_path / "AGENTS.md").read_text()
    first_mcp = (tmp_path / ".mcp.json").read_text()
    runner.invoke(app, ["setup-agent"])
    assert (tmp_path / "AGENTS.md").read_text() == first_agents
    assert (tmp_path / ".mcp.json").read_text() == first_mcp


def test_setup_agent_appends_claude_md(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "CLAUDE.md").write_text("# My project\n")
    runner.invoke(app, ["setup-agent"])
    content = (tmp_path / "CLAUDE.md").read_text()
    assert content.startswith("# My project\n")
    assert _LAKTORY_MARKER in content
    assert _LAKTORY_AGENTS_FILENAME in content


def test_setup_agent_merges_mcp(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    existing = {"mcpServers": {"other": {"command": "npx", "args": ["other"]}}}
    (tmp_path / ".mcp.json").write_text(json.dumps(existing))
    runner.invoke(app, ["setup-agent"])
    data = json.loads((tmp_path / ".mcp.json").read_text())
    assert "other" in data["mcpServers"]
    assert "laktory" in data["mcpServers"]
