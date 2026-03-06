import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild


def _str_to_timedelta(v):
    pattern = re.compile(r"(\d+)([smhd])")

    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    kwargs = {}

    for value, unit in pattern.findall(v):
        kwargs[units[unit]] = kwargs.get(units[unit], 0) + int(value)

    return timedelta(**kwargs)


class CronSchedule(BaseModel):
    """
    Cron Schedule.
    """

    cron: str = Field(
        ...,
        description="Cron expression defining when the DAG should be scheduled to run.",
    )
    timezone: str = Field(
        "utc", description="Timezone used to interpret the cron schedule."
    )


class AirflowOrchestrator(PipelineChild):
    """
    Airflow used as an orchestrator to execute a Laktory pipeline.

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    """

    type: Literal["AIRFLOW"] = Field("AIRFLOW", description="Type of orchestrator")
    description: str = Field(
        None, description="Optional human-readable description of the DAG."
    )
    schedule: CronSchedule = Field(
        None,
        description="Schedule that defines when the DAG should run. Uses a cron expression and timezone.",
    )  # simplified from airflow schedule options
    start_date: datetime = Field(
        None,
        description="The earliest date from which the scheduler will start creating DAG runs.",
    )
    end_date: datetime = Field(
        None, description="Optional date after which no new DAG runs will be scheduled."
    )
    template_searchpath: str | list[str] = Field(
        None,
        description="Directories to search for template files referenced in templated fields.",
    )
    # template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined  # not serializable
    user_defined_macros: dict = Field(
        None,
        description="Custom macros made available in the Jinja template context for all tasks in the DAG.",
    )
    user_defined_filters: dict = Field(
        None,
        description="Custom Jinja filters available when rendering templated fields.",
    )
    default_args: dict[str, Any] = Field(
        None,
        description="Dictionary of default arguments applied to all tasks in the DAG unless overridden.",
    )
    max_active_tasks: int = Field(
        None,
        description="Maximum number of task instances allowed to run concurrently across all DAG runs.",
    )
    max_active_runs: int = Field(
        None, description="Maximum number of active DAG runs allowed at the same time."
    )
    max_consecutive_failed_dag_runs: int = Field(
        None,
        description="Number of consecutive failed DAG runs allowed before the DAG is automatically paused.",
    )
    dagrun_timeout: str | timedelta = Field(
        None,
        description="Maximum time allowed for a DAG run to complete before it is marked as failed.",
    )
    catchup: bool = Field(
        None,
        description="Whether the scheduler should create DAG runs for all past schedule intervals between the start date and the current time.",
    )
    # on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None  # not serializable
    # on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None  # not serializable
    # deadline: list[DeadlineAlert] | DeadlineAlert = None   # not serializable
    doc_md: str = Field(
        None,
        description="Markdown documentation displayed in the Airflow UI for the DAG.",
    )
    # params: ParamsDict | dict[str, Any] = None  # forced by Laktory
    access_control: dict[str, dict[str, list[str]]] | dict[str, list[str]] = Field(
        None,
        description="Access control configuration defining roles and permissions for this DAG.",
    )
    is_paused_upon_creation: bool = Field(
        None, description="Whether the DAG should be paused when first created."
    )
    jinja_environment_kwargs: dict = Field(
        None,
        description="Additional keyword arguments used when creating the Jinja template environment.",
    )
    render_template_as_native_obj: bool = Field(
        None,
        description="If true, Jinja templates will render native Python objects instead of strings.",
    )
    tags: list[str] = Field(
        None, description="Tags used to organize and filter DAGs in the Airflow UI."
    )
    owner_links: dict[str, str] = Field(
        None,
        description="Mapping of DAG owners to URLs that provide additional information about them.",
    )
    auto_register: bool = Field(
        None,
        description="Whether the DAG should automatically be registered when parsed by Airflow.",
    )
    fail_fast: bool = Field(
        None,
        description="If enabled, the DAG run will stop scheduling new tasks as soon as a task fails.",
    )
    dag_display_name: str = Field(
        None,
        description="Human-friendly name displayed in the Airflow UI instead of the DAG ID.",
    )
    disable_bundle_versioning: bool = Field(
        None,
        description="If enabled, disables versioning of DAG bundles when deploying.",
    )

    @field_validator("dagrun_timeout", mode="before")
    def validate_timedelta(cls, v: Any) -> Any:
        return _str_to_timedelta(v)

    @field_validator("start_date", "end_date", mode="before")
    def validate_datetime(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
        return v

    @property
    def _schedule(self):
        schedule = self.schedule
        if isinstance(self.schedule, CronSchedule):
            from airflow.timetables.interval import CronDataIntervalTimetable

            schedule = CronDataIntervalTimetable(
                cron=schedule.cron,
                timezone=schedule.timezone,
            )

        return schedule

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def to_airflow(self, **dag_kwargs):
        from airflow.sdk import dag
        from airflow.sdk import get_current_context
        from airflow.sdk import task

        pl = self.parent_pipeline
        plan = pl.get_execution_plan()

        def register_task(pl_task):
            @task(task_id=pl_task.name)
            def airflow_task():
                ctx = get_current_context()

                # Values from dag_run.conf (Trigger DAG JSON)
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

                # Defaults from DAG params (if set)
                params = ctx.get("params") or {}

                # Let conf override params override hard defaults
                full_refresh = conf.get(
                    "full_refresh", params.get("full_refresh", False)
                )

                pl_task.execute(
                    full_refresh=bool(full_refresh),
                )

            return airflow_task

        kwargs = {
            "dag_id": pl.name,
            "params": {
                "full_refresh": False,
            },
        }
        for fname in self.model_fields_set:
            kwargs[fname] = getattr(self, fname)
            if fname == "schedule":
                kwargs[fname] = self._schedule
        for k, v in dag_kwargs.items():
            kwargs[k] = v

        @dag(**kwargs)
        def airflow_dag():
            tasks = {}
            nodes = {}

            # Create tasks and nodes
            for pl_task in plan.tasks:
                tasks[pl_task.name] = register_task(pl_task)
                nodes[pl_task.name] = tasks[pl_task.name]()

            # Set dependencies
            for task_name in tasks.keys():
                for upstream_task_name in plan.tasks_dict[
                    task_name
                ].upstream_task_names:
                    nodes[upstream_task_name] >> nodes[task_name]

        return airflow_dag()
