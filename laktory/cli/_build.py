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
    Build temporary files and python packages into `settings.build_root` if
    specified or default cache dir if not. These files may also be used when
    deployment is delegated to third parties like Databricks Declarative Bundles.

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
    laktory build --env dev
    laktory build --env dev --var profile=MY_PROFILE
    ```
    """
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        var_list=var,
        var_file_path=var_file,
    )

    # Call
    controller.build()
