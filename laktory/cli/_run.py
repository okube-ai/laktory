from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.dispatcher.dispatcher import Dispatcher


@app.command()
def run(
    databricks_job: Annotated[
        str, typer.Option("--dbks-job", "-dbksj", help="Job name")
    ] = None,
    databricks_pipeline: Annotated[
        str, typer.Option("--dbks-pipeline", "-dbksp", help="Job name")
    ] = None,
    timeout: Annotated[
        float,
        typer.Option(
            "--timeout", "-t", help="Maximum allowed time (in seconds) for run"
        ),
    ] = 1200,
    raise_exception: Annotated[
        bool, typer.Option("--raise", "-r", help="Raise exception on failure")
    ] = True,
    full_refresh: Annotated[
        bool,
        typer.Option(
            "--full-refresh", "--fr", help="Full tables refresh (pipeline only)"
        ),
    ] = False,
    current_run_action: Annotated[
        str,
        typer.Option(
            "--action",
            "-a",
            help="Action to take if job currently running ['WAIT', 'CANCEL', 'FAIL']",
        ),
    ] = "WAIT",
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
):
    """
    Execute remote job or DLT pipeline and monitor failures until completion.

    Parameters
    ----------
    databricks_job:
        Name of the job to run (mutually exclusive with dlt)
    databricks_pipeline:
        Name of the DLT pipeline to run (mutually exclusive with job)
    timeout:
        Maximum allowed time (in seconds) for run.
    raise_exception:
        Raise exception on failure
    current_run_action:
        Action to take for currently running job or pipline.
    full_refresh:
        Full tables refresh (pipline only)
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.

    Examples
    --------
    ```cmd
    laktory run --env dev --dbks-pipeline pl-stock-prices --full_refresh --action CANCEL
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)

    """

    # Set Resource Name
    if databricks_job and databricks_pipeline:
        raise ValueError("Only one of `job` or `dlt` should be set.")
    if not (databricks_job or databricks_pipeline):
        raise ValueError("One of `job` or `dlt` should be set.")

    # Set Dispatcher
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
    )
    dispatcher = Dispatcher(stack=controller.stack)
    dispatcher.get_resource_ids()

    if databricks_job:
        dispatcher.run_databricks_job(
            job_name=databricks_job,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
        )

    if databricks_pipeline:
        dispatcher.run_databricks_dlt(
            dlt_name=databricks_pipeline,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
            full_refresh=full_refresh,
        )
