from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app


@app.command()
def build(
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
):
    """
    Build temporary files and python packages.

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.

    Examples
    --------
    ```cmd
    laktory build --env dev
    ```
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
    )

    # Call
    controller.build()
