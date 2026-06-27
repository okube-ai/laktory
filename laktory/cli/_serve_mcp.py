from laktory._logger import get_logger
from laktory.cli.app import app

logger = get_logger(__name__)


@app.command()
def serve_mcp():
    """
    Start the Laktory MCP server (stdio transport).

    Requires the `mcp` optional extra: `pip install laktory[mcp]`

    Examples
    --------
    ```cmd
    laktory serve-mcp
    ```

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

    References
    ----------
    * [MCP](https://modelcontextprotocol.io/)
    """
    from laktory.mcp.server import mcp

    mcp.run(transport="stdio")
