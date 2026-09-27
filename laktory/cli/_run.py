from typing import Annotated

import typer

from laktory.cli._common import CLIController
from laktory.cli.app import app
from laktory.dispatcher.dispatcher import Dispatcher


@app.command()
def run(
    databricks_job: Annotated[
        str, typer.Option("--databricks-job", help="Databricks job name")
    ] = None,
    databricks_pipeline: Annotated[
        str, typer.Option("--databricks-pipeline", help="Databricks pipeline name")
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
    refresh: Annotated[
        str,
        typer.Option(
            "--refresh",
            help="What the run does: 'INCREMENTAL', 'FULL' (reset, then reprocess all the "
            "data) or 'RESET' (reset only, jobs only). Defaults to the job / pipeline "
            "default (incremental).",
        ),
    ] = None,
    reset_mode: Annotated[
        str,
        typer.Option(
            "--reset-mode",
            help="Override of the sinks reset mode ('DROP' or 'TRUNCATE'), with "
            "--refresh FULL or RESET (jobs only).",
        ),
    ] = None,
    tasks: Annotated[
        str,
        typer.Option(
            "--tasks",
            help="Comma-separated keys of the job tasks to run. Defaults to all the "
            "tasks (jobs only).",
        ),
    ] = None,
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
    refresh:
        What the run does: `INCREMENTAL`, `FULL` (reset, then reprocess all the data)
        or `RESET` (reset only, jobs only). Defaults to the job / pipeline default.
    reset_mode:
        Override of the sinks reset mode (`DROP` or `TRUNCATE`), with `refresh` `FULL`
        or `RESET` (jobs only).
    tasks:
        Comma-separated keys of the job tasks to run (jobs only). Defaults to all the
        tasks.
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
    laktory run --env dev --databricks-pipeline pl-stock-prices --refresh FULL --action CANCEL
    laktory run --env dev --databricks-job my-job --var profile=MY_PROFILE
    laktory run --env dev --databricks-job my-job --refresh FULL --tasks node-slv_prices
    laktory run --env dev --databricks-job my-job --refresh RESET --reset-mode DROP
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

    # Run parameters
    refresh, reset_mode, task_keys = _validate_run_options(
        refresh, reset_mode, tasks, is_job=databricks_job is not None
    )

    # Set Dispatcher
    controller = CLIController(
        env=environment,
        stack_filepath=filepath,
        var_list=var,
        var_file_path=var_file,
    )
    dispatcher = Dispatcher(stack=controller.stack, env=controller.env)
    dispatcher.get_resource_ids()

    if databricks_job:
        job_parameters = {}
        if refresh:
            job_parameters["refresh"] = refresh
        if reset_mode:
            job_parameters["reset_mode"] = reset_mode
        dispatcher.run_databricks_job(
            job_name=databricks_job,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
            job_parameters=job_parameters or None,
            only=task_keys,
        )

    if databricks_pipeline:
        dispatcher.run_databricks_pipeline(
            pipeline_name=databricks_pipeline,
            timeout=timeout,
            raise_exception=raise_exception,
            current_run_action=current_run_action,
            full_refresh=refresh == "FULL",
        )


def _validate_run_options(
    refresh: str | None, reset_mode: str | None, tasks: str | None, is_job: bool
) -> tuple[str | None, str | None, list[str] | None]:
    """Validate and normalize the run options before starting anything."""
    if refresh:
        refresh = refresh.upper()
        if refresh not in ["INCREMENTAL", "FULL", "RESET"]:
            raise ValueError(
                f"`--refresh` '{refresh}' is not supported. Use 'INCREMENTAL', 'FULL' or "
                "'RESET'."
            )

    if reset_mode:
        reset_mode = reset_mode.upper()
        if reset_mode not in ["DROP", "TRUNCATE"]:
            raise ValueError(
                f"`--reset-mode` '{reset_mode}' is not supported. Use 'DROP' or 'TRUNCATE'."
            )
        if refresh not in ["FULL", "RESET"]:
            raise ValueError("`--reset-mode` requires `--refresh FULL` or `RESET`.")

    task_keys = [t.strip() for t in (tasks or "").split(",") if t.strip()] or None

    if not is_job:
        # Declarative pipelines: the engine resets the tables
        unsupported = []
        if refresh == "RESET":
            unsupported += ["`--refresh RESET`"]
        if reset_mode:
            unsupported += ["`--reset-mode`"]
        if task_keys:
            unsupported += ["`--tasks`"]
        if unsupported:
            raise ValueError(
                f"{', '.join(unsupported)} not supported with `--databricks-pipeline`: "
                "declarative pipelines are refreshed by their engine. Use `--refresh FULL`."
            )

    return refresh, reset_mode, task_keys
