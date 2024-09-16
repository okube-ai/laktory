import typer
from typing import Annotated

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.constants import SUPPORTED_BACKENDS


@app.command()
def destroy(
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    auto_approve: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Automatically approve and perform the destroy after previewing it",
        ),
    ] = False,
    options: Annotated[
        str,
        typer.Option("--options", help="Comma separated IaC backend options (flags)."),
    ] = None,
):
    """
    Destroy all remote objects managed by the stack

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    auto_approve:
        Automatically approve and perform the destroy after previewing it
    options:
        Comma separated IaC backend options (flags).

    Examples
    --------
    ```cmd
    laktory destroy --env dev
    ```

    References
    ----------
    - terraform [destroy](https://developer.hashicorp.com/terraform/cli/commands/destroy)
    - pulumi [destroy](https://www.pulumi.com/docs/cli/commands/pulumi_destroy/)
    """
    controller = CLIController(
        env=environment,
        auto_approve=auto_approve,
        stack_filepath=filepath,
        options_str=options,
    )

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("destroy")
    elif controller.backend == "terraform":
        controller.terraform_call("destroy")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")
