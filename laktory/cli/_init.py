import typer
from typing import Annotated

from laktory.cli._common import CLIController

from laktory.cli.app import app


@app.command()
def init(
    backend: Annotated[str, typer.Option(help="IaC backend [terraform]")] = None,
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
    Initialize IaC backend

    Parameters
    ----------
    backend:
        IaC backend [terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

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
        backend=backend,
        organization=organization,
        env=environment,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        raise ValueError("Pulumi backend not supported for init command")
    elif controller.backend == "terraform":
        controller.terraform_call("init")
    else:
        raise ValueError("backend should be ['terraform']")
