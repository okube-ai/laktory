from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app


@app.command()
def validate(
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
):
    """
    Validate configuration and resources.

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.

    Examples
    --------
    ```cmd
    laktory validate --env dev
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    """
    CLIController(
        env=environment,
        stack_filepath=filepath,
    )
