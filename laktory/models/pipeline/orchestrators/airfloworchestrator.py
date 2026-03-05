import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory.models.pipelinechild import PipelineChild

ScheduleInterval = str | timedelta
# try:
#     from dateutil.relativedelta import relativedelta
#     ScheduleInterval = ScheduleInterval | relativedelta  # not serializable
# except (ModuleNotFoundError, ImportError):
#     pass

# try:
#     from airflow.timetables.base import Timetable
#     # ScheduleInterval = ScheduleInterval | Timetable  # not serializable
# except (ModuleNotFoundError, ImportError):
#     pass

# try:
#     from airflow.sdk.definitions.asset import BaseAsset
#     # ScheduleInterval = ScheduleInterval | BaseAsset | list[BaseAsset]  # not serializable
# except (ModuleNotFoundError, ImportError):
#     pass


class AirflowOrchestrator(PipelineChild):
    """
    Airflow used as an orchestrator to execute a Laktory pipeline.

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    """

    type: Literal["AIRFLOW"] = Field("AIRFLOW", description="Type of orchestrator")
    description: str = None
    schedule: ScheduleInterval = None
    # schedule: str | timedelta |  = None
    start_date: datetime = None
    end_date: datetime = None
    template_searchpath: str | list[str] = None
    # template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined  # not serializable
    user_defined_macros: dict = None
    user_defined_filters: dict = None
    default_args: dict[str, Any] = None
    max_active_tasks: int = None
    max_active_runs: int = None
    max_consecutive_failed_dag_runs: int = None
    dagrun_timeout: str | timedelta = None
    catchup: bool = None
    # on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None  # not serializable
    # on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None  # not serializable
    # deadline: list[DeadlineAlert] | DeadlineAlert = None   # not serializable
    doc_md: str = None
    # params: ParamsDict | dict[str, Any] = None  # forced by Laktory
    access_control: dict[str, dict[str, list[str]]] | dict[str, list[str]] = None
    is_paused_upon_creation: bool = None
    jinja_environment_kwargs: dict = None
    render_template_as_native_obj: bool = None
    tags: list[str] = None
    owner_links: dict[str, str] = None
    auto_register: bool = None
    fail_fast: bool = None
    dag_display_name: str = None
    disable_bundle_versioning: bool = None

    @field_validator("dagrun_timeout", "schedule", mode="before")
    def validate_timedelta(cls, v: Any) -> Any:
        pattern = re.compile(r"(\d+)([smhd])")

        units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
        kwargs = {}

        for value, unit in pattern.findall(v):
            kwargs[units[unit]] = kwargs.get(units[unit], 0) + int(value)

        return timedelta(**kwargs)

    @field_validator("start_date", "end_date", mode="before")
    def validate_datetime(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
        return v

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
