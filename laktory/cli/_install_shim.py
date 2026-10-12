import sys
from pathlib import Path

import typer

from laktory._logger import get_logger
from laktory.cli.app import app

logger = get_logger(__name__)


@app.command()
def install_shim():
    """
    Write a `laktory-safe` wrapper script into the current environment's
    scripts directory that calls `python -m laktory` directly, bypassing
    the pip-generated `laktory` console-script executable.

    Useful when a security policy (e.g. Microsoft Defender Attack Surface
    Reduction rules) blocks newly generated, unsigned executables such as
    the `laktory.exe` launcher pip creates for console_scripts.

    Examples
    --------
    ```cmd
    laktory install-shim
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    """
    scripts_dir = Path(sys.executable).parent
    if sys.platform == "win32":
        target = scripts_dir / "laktory-safe.cmd"
        target.write_text('@echo off\r\n"%~dp0python.exe" -m laktory %*\r\n')
    else:
        target = scripts_dir / "laktory-safe"
        target.write_text(f'#!/bin/sh\nexec "{sys.executable}" -m laktory "$@"\n')
        target.chmod(0o755)
    typer.echo(f"Wrote wrapper: {target}")
