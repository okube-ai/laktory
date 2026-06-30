??? "API Documentation"
    [`laktory.cli.setup_agent`](../api/cli.md)<br>

Laktory ships with first-class support for AI coding agents such as
[Claude Code](https://claude.ai/code) and [GitHub Copilot](https://github.com/features/copilot).
When configured, these agents can read Laktory's full model documentation, validate YAML
snippets against live schemas, and discover exact field names and types — all without
leaving the coding environment.

## Two tiers of support

`setup-agent` provides two independent layers of value:

**Tier 1 — Instruction file** (always written): A Markdown reference document is placed
where the agent automatically loads it at session start. It contains YAML syntax, common
patterns, model hierarchy, naming conventions, and variable injection examples. This
works in every environment, with no server process required.

**Tier 2 — MCP server** (written by default, opt-out with `--no-mcp`): A `.mcp.json`
file tells the agent to start the Laktory MCP server alongside the coding session. This
gives the agent live access to exact model schemas, field-level documentation, and YAML
validation against the installed Laktory version.

## Setup

Run `laktory setup-agent` in the root of your project:

```cmd
laktory setup-agent
```

The command prompts for the agent framework you are using and then writes the appropriate
instruction and configuration files:

| Agent | Instruction file                               | Host file updated |
|---|------------------------------------------------|---|
| `claude` | `.claude/docs/laktory.md`                      | `CLAUDE.md` (imports the instruction file) |
| `copilot` | `.github/instructions/laktory.instructions.md` | `AGENTS.md` (links to the instruction file) |
| `other` | `AGENTS_LAKTORY.md`                            | `AGENTS.md` (links to the instruction file) |

By default, a `.mcp.json` file is also written to register the Laktory MCP server.

Use `--agent` to skip the interactive prompt, and `--no-mcp` to write only the
instruction files without MCP server configuration:

```cmd
laktory setup-agent --agent claude
laktory setup-agent --agent claude --no-mcp
```

All writes are idempotent — running `setup-agent` more than once is safe.

!!! tip
    Use `--no-mcp` in environments where running background server processes is
    restricted or not permitted. The instruction file alone still provides significant
    value for YAML authoring.

## MCP Server

The instruction files tell the agent how to use Laktory's YAML syntax. The **MCP server**
gives the agent live access to the model schemas of the installed Laktory version. It
exposes five tools:

| Tool | Purpose |
|---|---|
| `list_models()` | List all queryable models grouped by category |
| `get_model_docs(model_name)` | Full field reference for a model; nested sub-model schemas are inlined automatically |
| `validate_yaml(yaml_content, model_name=None)` | Validate any Laktory model YAML; auto-detects Pipeline/Stack when `model_name` is omitted |
| `get_laktory_docs()` | Return the full Laktory AI agent reference (AGENTS.md) with patterns and examples |
| `get_version()` | Return the installed Laktory version |

The `.mcp.json` written by `setup-agent` starts the server via:

```json
{
  "mcpServers": {
    "laktory": {
      "command": "python",
      "args": ["-m", "laktory.mcp.server"]
    }
  }
}
```

!!! note
    The MCP server requires the optional `mcp` dependency. Install it with:
    ```cmd
    pip install laktory[mcp]
    ```

## Agent workflow

When an MCP server is active, a capable agent will:

1. Call `get_laktory_docs()` when working with a resource type for the first time to
   load YAML patterns and examples for that resource category.
2. Call `get_model_docs(model_name)` to get the exact field names, types, and defaults for
   the model it is about to configure. Nested sub-models (e.g. `ClusterInitScripts`
   inside `Cluster`) are included inline — no follow-up call needed.
3. Generate the YAML and call `validate_yaml` to catch schema errors before writing to
   the file. For Pipeline or Stack YAML, `model_name` can be omitted (auto-detected).
   For any other model, pass it explicitly: `validate_yaml(yaml, model_name="Cluster")`.

For small incremental edits to existing files (e.g. adding a user to a group), reading
the surrounding codebase context is sufficient. The MCP server is most valuable when
creating new resource or pipeline blocks from scratch.

## Agent instructions

The instruction file written by `setup-agent` is sourced from `laktory/AGENTS.md` in the
installed package. It contains the full Laktory reference for AI agents, including:

- Core YAML concepts and composition tags (`!use`, `!extend`, `!update`)
- Stack structure and model hierarchy
- Key model field reference tables
- Common pipeline and resource YAML patterns
- Naming conventions and variable injection syntax

The file is automatically overwritten on each `setup-agent` run, so it always reflects
the installed version.
