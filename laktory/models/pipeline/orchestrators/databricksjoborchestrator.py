from typing import Literal

from pydantic import Field
from pydantic import PrivateAttr

from laktory._logger import get_logger
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipeline.orchestrators.sqlexecutionworkspacefile import (
    SqlExecutionWorkspaceFile,
)
from laktory.models.pipelinechild import PipelineChild
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.job import JobEnvironment
from laktory.models.resources.databricks.job import JobEnvironmentSpec
from laktory.models.resources.databricks.job import JobParameter
from laktory.models.resources.databricks.job import JobTask
from laktory.models.resources.databricks.job import JobTaskLibrary
from laktory.models.resources.databricks.job import JobTaskLibraryPypi
from laktory.models.resources.databricks.job import JobTaskPythonWheelTask
from laktory.models.resources.databricks.job import JobTaskSqlTask
from laktory.models.resources.databricks.job import JobTaskSqlTaskFile

logger = get_logger(__name__)

ENV_KEY = "laktory"


class DatabricksJobOrchestrator(Job, PipelineChild):
    """
    Databricks job used as an orchestrator to execute a Laktory pipeline.

    Job orchestrator supports incremental workloads with Spark Structured
    Streaming, but it does not support continuous processing.

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
    """

    type: Literal["DATABRICKS_JOB"] = Field(
        "DATABRICKS_JOB", description="Type of orchestrator"
    )
    config_file: PipelineConfigWorkspaceFile = Field(
        PipelineConfigWorkspaceFile(),
        description="Pipeline configuration (json) file deployed to the workspace and used by the job to read and execute the pipeline.",
    )
    node_max_retries: int = Field(
        None,
        description="An optional maximum number of times to retry an unsuccessful run for each node.",
    )
    serverless_environment_version: str = Field(
        None, description="Serverless environment version"
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the SQL warehouse. When set, each pipeline execution task is automatically "
        "assigned to the warehouse if all its nodes are SQL-compatible (single DataFrameExpr transformer "
        "with TableDataSink sinks). Tasks with Python-based transformations fall back to "
        "cluster or serverless compute.",
    )
    _sql_files: list = PrivateAttr(default_factory=list)
    _needs_python: bool = PrivateAttr(default=True)

    # ----------------------------------------------------------------------- #
    # Update Job                                                              #
    # ----------------------------------------------------------------------- #

    def _set_task_compute(self, task, serverless, _requirements):
        if serverless:
            task.environment_key = ENV_KEY
        else:
            libraries = []
            for r in _requirements:
                is_var = "${vars." in r
                if r.endswith(".whl") or (is_var and "wheel" in r or "whl" in r):
                    l = JobTaskLibrary(whl=r)
                else:
                    l = JobTaskLibrary(pypi=JobTaskLibraryPypi(package=r))
                libraries += [l]

            task.job_cluster_key = "node-cluster"
            task.library = libraries
        return task

    def _build_sql_task(self, pl_task, depends_on):
        sql_file = SqlExecutionWorkspaceFile(task_name=pl_task.name)
        sql_file._parent = self
        self._sql_files.append(sql_file)

        return JobTask(
            task_key=pl_task.name,
            sql_task=JobTaskSqlTask(
                warehouse_id=self.warehouse_id,
                file=JobTaskSqlTaskFile(
                    path=f"/Workspace{sql_file.path}",
                    source="WORKSPACE",
                ),
            ),
            depends_on=depends_on,
        )

    def _build_python_task(self, pl_task, depends_on, _path, serverless, _requirements):
        task = JobTask(
            task_key=pl_task.name,
            python_wheel_task=JobTaskPythonWheelTask(
                entry_point="models.pipeline._execute",
                package_name="laktory",
                named_parameters={
                    "filepath": _path,
                    "selects": ",".join(pl_task.node_names),
                },
            ),
            depends_on=depends_on,
        )
        task = self._set_task_compute(task, serverless, _requirements)
        if self.node_max_retries:
            task.max_retries = self.node_max_retries
        return task

    def update_from_parent(self):
        self._sql_files = []
        pl = self.parent_pipeline
        self.task = []

        # Execution plan (sorted for stable output)
        plan = pl.get_execution_plan()
        pl_tasks = plan.tasks_dict
        pl_task_names = sorted(pl_tasks.keys())

        # Determine which tasks can run on a SQL warehouse
        sql_task_names: set[str] = set()
        if self.warehouse_id:
            for name in pl_task_names:
                if pl_tasks[name].is_sql_expressible:
                    sql_task_names.add(name)

        has_python_tasks = (
            any(n not in sql_task_names for n in pl_task_names)
            or pl.databricks_quality_monitor_enabled
        )
        self._needs_python = has_python_tasks

        # Python compute setup (cluster or serverless) — only when needed
        serverless = True
        _requirements = []
        _path = None
        if has_python_tasks:
            if self.job_cluster:
                for c in self.job_cluster:
                    if c.job_cluster_key == "node-cluster":
                        serverless = False
                if len(self.job_cluster) > 0 and serverless:
                    raise ValueError(
                        "To use DATABRICKS_JOB orchestrator, a cluster named `node-cluster` must be defined in the databricks_job attribute."
                    )

            _requirements = self.inject_vars_into_dump({"deps": pl._dependencies})[
                "deps"
            ]
            _path = (
                "/Workspace"
                + self.inject_vars_into_dump({"path": self.config_file.path})["path"]
            )

            # Environment (serverless only)
            env_found = False
            envs = self.environment or []
            for env in envs:
                if env.environment_key == ENV_KEY:
                    env_found = True
                    break

            if not env_found:
                if serverless and not self.serverless_environment_version:
                    raise ValueError(
                        "To use serverless a `serverless_environment_version` must be specified."
                    )
                _version = self.serverless_environment_version or "5"
                envs += [
                    JobEnvironment(
                        environment_key=ENV_KEY,
                        spec=JobEnvironmentSpec(
                            dependencies=_requirements,
                            environment_version=_version,
                        ),
                    )
                ]
                self.environment = envs

        # Build tasks
        for pl_task_name in pl_task_names:
            pl_task = pl_tasks[pl_task_name]
            depends_on = [{"task_key": n} for n in pl_task.upstream_task_names]

            if pl_task_name in sql_task_names:
                task = self._build_sql_task(pl_task, depends_on)
            else:
                task = self._build_python_task(
                    pl_task, depends_on, _path, serverless, _requirements
                )

            self.task += [task]

        if pl.databricks_quality_monitor_enabled:
            task = JobTask(
                task_key="post-execute",
                python_wheel_task=JobTaskPythonWheelTask(
                    entry_point="models.pipeline._post_execute",
                    package_name="laktory",
                    named_parameters={
                        "filepaths": _path,
                        "tables_metadata": "false",
                        "quality_monitors": "true",
                    },
                ),
                depends_on=[{"task_key": t.task_key} for t in self.task],
            )
            task = self._set_task_compute(task, serverless, _requirements)
            self.task += [task]

        self.sort_tasks(self.task)

        if has_python_tasks:
            self.parameter = [
                JobParameter(name="full_refresh", default="false"),
            ]

    # ----------------------------------------------------------------------- #
    # DABs                                                                    #
    # ----------------------------------------------------------------------- #

    def to_dab_resource(self):
        """
        Convert to a DABs Python Job resource object for use with
        ``laktory.dab.build_resources``.

        Returns
        -------
        :
            ``databricks.bundles.jobs.Job`` instance.
        """
        from databricks.bundles.jobs import Job as DabsJob

        d = self.model_dump(
            exclude=self.terraform_excludes, exclude_unset=True, by_alias=False
        )
        for task in d.get("task", []):
            # schema_ is a Python workaround for the reserved name; DABs expects "schema"
            if "dbt_task" in task and "schema_" in task["dbt_task"]:
                task["dbt_task"]["schema"] = task["dbt_task"].pop("schema_")
            # DABs SDK uses plural names for list fields
            if "library" in task:
                task["libraries"] = task.pop("library")

        # DABs SDK uses plural names for top-level list fields
        for singular, plural in [
            ("task", "tasks"),
            ("job_cluster", "job_clusters"),
            ("environment", "environments"),
            ("parameter", "parameters"),
        ]:
            if singular in d:
                d[plural] = d.pop(singular)

        return DabsJob.from_dict(d)

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["config_file"]

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        return "job"

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return super().terraform_excludes + [
            "config_file",
            "node_max_retries",
            "type",
            "dataframe_backend",
            "dataframe_api",
            "serverless_environment_version",
            "warehouse_id",
        ]

    @property
    def additional_core_resources(self) -> list:
        """
        - configuration workspace file (python-wheel mode)
        - SQL execution workspace files (warehouse mode)
        - configuration workspace file permissions
        """

        resources = super().additional_core_resources
        if self._needs_python:
            resources += [self.config_file]
        resources += self._sql_files

        return resources
