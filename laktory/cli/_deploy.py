import typer
from typing import Annotated

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.constants import SUPPORTED_BACKENDS


@app.command()
def deploy(
    backend: Annotated[
        str, typer.Option(help="IaC backend [pulumi, terraform]")
    ] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
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
    pulumi_options: Annotated[
        str,
        typer.Option(
            "--pulumi-options", help="Comma separated pulumi options (flags)."
        ),
    ] = None,
    terraform_options: Annotated[
        str,
        typer.Option(
            "--terraform-options", help="Comma separated terraform options (flags)."
        ),
    ] = None,
):
    """
    Execute deployment.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    auto_approve:
        Automatically approve and perform the update after previewing it
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory deploy --env dev --filepath my-stack.yaml
    ```

    References
    ----------
    - pulumi up [options](https://www.pulumi.com/docs/cli/commands/pulumi_up/)
    """
    controller = CLIController(
        backend=backend,
        organization=organization,
        env=environment,
        auto_approve=auto_approve,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("up")
    elif controller.backend == "terraform":
        controller.terraform_call("apply")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")
