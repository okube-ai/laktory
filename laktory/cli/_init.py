from typing import Annotated

import typer

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
    Initialize IaC backend

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
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
    laktory init --env dev
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    * terraform [init](https://developer.hashicorp.com/terraform/cli/commands/init)
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        options_str=options,
        var_list=var,
        var_file_path=var_file,
    )

    # Call
    if controller.iac_backend == "terraform":
        controller.terraform_call("init")
    else:
        raise ValueError("backend should be ['terraform']")
