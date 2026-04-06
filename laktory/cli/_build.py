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

    Pipeline config JSON files are written to the location set by
    ``settings.laktory_build_dir`` in ``stack.yaml`` (or the default Laktory
    cache when not set). For Databricks Asset Bundles users, set
    ``settings.laktory_build_dir: .resources/`` in ``stack.yaml`` so that DABs can
    sync the built files to the workspace.

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
