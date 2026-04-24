from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.constants import SUPPORTED_BACKENDS


@app.command()
def preview(
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    options: Annotated[
        str,
        typer.Option("--options", help="Comma separated IaC backend options (flags)."),
    ] = None,
):
    """
    Validate configuration and resources and preview deployment.

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    options:
        Comma separated IaC backend options (flags).

    Examples
    --------
    ```cmd
    laktory preview --env dev
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    * terraform [plan](https://developer.hashicorp.com/terraform/cli/commands/plan)
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        options_str=options,
    )

    # Call
    if controller.backend == "terraform":
        controller.terraform_call("plan")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")
