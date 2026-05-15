from typing import Annotated

import typer

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
    var: Annotated[
        list[str],
        typer.Option("--var", help="Variable override as key=value. Can be repeated."),
    ] = [],
    var_file: Annotated[
        str,
        typer.Option(
            "--var-file",
            help="YAML file of variable overrides. Auto-discovers variables[.{env}].yaml if not set.",
        ),
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
    var:
        Variable override as `key=value`. Can be repeated. Overrides variables
        defined in the stack YAML and in `--var-file`.
    var_file:
        Path to a YAML file of variable overrides. If not provided, a
        `variables[.{env}].yaml` file next to the stack file is used automatically
        when present.

    Examples
    --------
    ```cmd
    laktory deploy --env dev --filepath my-stack.yaml
    laktory deploy --env dev --var profile=MY_PROFILE --var node_type=Standard_DS3_v2
    laktory deploy --env dev --var-file variables.yaml
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    * terraform [apply](https://developer.hashicorp.com/terraform/cli/commands/apply)
    """
    controller = CLIController(
        env=environment,
        auto_approve=auto_approve,
        stack_filepath=filepath,
        options_str=options,
        var_list=var,
        var_file_path=var_file,
    )

    # Call
    if controller.iac_backend == "terraform":
        controller.terraform_call("apply")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")
