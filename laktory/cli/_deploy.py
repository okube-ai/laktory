import typer
from typing import Annotated

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.constants import SUPPORTED_BACKENDS


@app.command()
def deploy(
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
            help="Automatically approve and perform the update after previewing it",
        ),
    ] = False,
    options: Annotated[
        str,
        typer.Option("--options", help="Comma separated IaC backend options (flags)."),
    ] = None,
):
    """
    Execute deployment.

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    auto_approve:
        Automatically approve and perform the update after previewing it
    options:
        Comma separated IaC backend options (flags).

    Examples
    --------
    ```cmd
    laktory deploy --env dev --filepath my-stack.yaml
    ```

    References
    ----------
    - pulumi [up](https://www.pulumi.com/docs/cli/commands/pulumi_up/)
    - terraform [apply](https://developer.hashicorp.com/terraform/cli/commands/apply)
    """
    controller = CLIController(
        env=environment,
        auto_approve=auto_approve,
        stack_filepath=filepath,
        options_str=options,
    )

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("up")
    elif controller.backend == "terraform":
        controller.terraform_call("apply")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")
