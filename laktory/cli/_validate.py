from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app


@app.command()
def validate(
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
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
    Validate configuration and resources.

    Parameters
    ----------
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
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
    laktory validate --env dev
    laktory validate --env dev --var profile=MY_PROFILE
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        var_list=var,
        var_file_path=var_file,
    )

    # Shortcut to call resources cross-reference validation (_check_depends_on)
    controller.stack.to_terraform(
        env_name=controller.env, vars=controller.cli_vars or None
    )
