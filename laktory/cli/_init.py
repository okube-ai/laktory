import typer
from typing import Annotated

from laktory.cli._common import CLIController

from laktory.cli.app import app


@app.command()
def init(
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
    Initialize IaC backend

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
    laktory init --env dev
    ```

    References
    ----------
    - terraform [init](https://developer.hashicorp.com/terraform/cli/commands/init)
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        options_str=options,
    )

    # Call
    if controller.backend == "pulumi":
        raise ValueError("Pulumi backend not supported for init command")
    elif controller.backend == "terraform":
        controller.terraform_call("init")
    else:
        raise ValueError("backend should be ['terraform']")
