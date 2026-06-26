from __future__ import annotations

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:
    raise ImportError(
        "The 'mcp' package is required to run the Laktory MCP server. "
        "Install it with: pip install laktory[mcp]"
    ) from exc

from laktory.mcp._model_docs import get_laktory_docs as _get_laktory_docs
from laktory.mcp._model_docs import get_model_docs as _get_model_docs
from laktory.mcp._model_docs import list_models as _list_models
from laktory.mcp._validate import validate_yaml as _validate_yaml

mcp = FastMCP("laktory")


@mcp.tool()
def get_model_docs(model_name: str) -> str:
    """Return a Markdown field-reference table for a Laktory model.

    Use this before writing YAML for any Laktory model to discover its fields,
    types, defaults, and descriptions.
    """
    return _get_model_docs(model_name)


@mcp.tool()
def validate_yaml(yaml_content: str) -> dict:
    """Validate a Laktory YAML snippet.

    Auto-detects whether the content is a Pipeline or a Stack and runs Pydantic
    validation. Returns {"valid": true} on success or {"valid": false, "errors": [...]}
    on failure.
    """
    return _validate_yaml(yaml_content)


@mcp.tool()
def list_models() -> dict:
    """List all Laktory models grouped by category.

    Returns a dict with categories (pipeline, sources, sinks, orchestrators,
    stack) as keys and lists of model names as values.
    """
    return _list_models()


@mcp.tool()
def get_laktory_docs() -> str:
    """Return the full Laktory AI Agent reference (AGENTS.md).

    Call this once to get all patterns, YAML examples, naming conventions,
    model hierarchy, and variable injection syntax in one response.
    Prefer get_model_docs for field-level precision on a specific model.
    """
    return _get_laktory_docs()


@mcp.tool()
def get_version() -> dict:
    """Return the installed Laktory version."""
    from laktory._version import VERSION

    return {"version": VERSION}


if __name__ == "__main__":
    mcp.run(transport="stdio")
