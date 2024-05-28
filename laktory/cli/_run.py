import typer
from typing import Annotated

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.dispatcher.dispatcher import Dispatcher


@app.command()
def run(
    job: Annotated[str, typer.Option("--job", "-j", help="Job name")] = None,
    dlt: Annotated[str, typer.Option("--dlt", "-dlt", help="Job name")] = None,
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
    job:
        Name of the job to run (mutually exclusive with dlt)
    dlt:
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
    laktory run --env dev --dlt pl-stock-prices --full_refresh --action CANCEL
    ```

    """

    # Set Resource Name
    if job and dlt:
        raise ValueError("Only one of `job` or `dlt` should be set.")
    if not (job or dlt):
        raise ValueError("One of `job` or `dlt` should be set.")

    # Set Dispatcher
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
    )
    dispatcher = Dispatcher(stack=controller.stack)
    dispatcher.get_resource_ids()

    if job:
        dispatcher.run_job(
            job_name=job,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
        )

    if dlt:
        dispatcher.run_dlt(
            dlt_name=dlt,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
            full_refresh=full_refresh,
        )
