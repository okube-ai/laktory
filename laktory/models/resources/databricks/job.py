import json
from typing import Any
from typing import Literal
from typing import Union

from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.cluster import ClusterLibrary
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class JobJobClusterNewCluster(Cluster):
    """
    Job Cluster. Same attributes as `laktory.models.Cluster`, except for

    * `access_controls`
    * `is_pinned`
    * `libraries`
    * `name`
    * `no_wait`

    that are not allowed.
    """

    access_controls: list[Any] = Field(None, exclude=True)
    is_pinned: bool = Field(None, exclude=True)
    libraries: list[Any] = Field(None, exclude=True)
    name: str = Field(None, exclude=True)
    no_wait: bool = Field(None, exclude=True)

    @model_validator(mode="after")
    def excluded_fields(self) -> Any:
        for f in [
            "access_controls",
            "is_pinned",
            "libraries",
            "name",
            "no_wait",
        ]:
            if getattr(self, f, None) not in [None, [], {}]:
                raise ValueError(f"Field {f} should be null")

        return self

    @property
    def resource_key(self) -> str:
        return ""


class JobJobCluster(BaseModel):
    job_cluster_key: str = Field(
        ...,
        description="Identifier that can be referenced in task block, so that cluster is shared between tasks",
    )
    new_cluster: JobJobClusterNewCluster = Field(
        ...,
        description="""
    Block with almost the same set of parameters as for databricks.Cluster resource, except following (check the REST
    API documentation for full list of supported parameters):
    """,
    )


class JobContinuous(BaseModel):
    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str] = Field(
        None,
        description="""
    Indicate whether this continuous job is paused or not. When the pause_status field is omitted in the block,
    the server will default to using `UNPAUSED` as a value for pause_status.
    """,
    )


class JobEmailNotifications(BaseModel):
    no_alert_for_skipped_runs: bool = Field(
        None,
        description="""
    If `True`, don't send alert for skipped runs. (It's recommended to use the corresponding setting in the
    notification_settings configuration block).
    """,
    )
    on_duration_warning_threshold_exceededs: list[str] = Field(
        None,
        description="""
    List of emails to notify when the duration of a run exceeds the threshold specified by the RUN_DURATION_SECONDS
    metric in the health block.
    """,
    )
    on_failures: list[str] = Field(
        None, description="List of emails to notify when the run fails."
    )
    on_starts: list[str] = Field(
        None, description="List of emails to notify when the run starts."
    )
    on_successes: list[str] = Field(
        None,
        description="List of emails to notify when the run completes successfully.",
    )

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "on_failures": "on_failure",
            "on_starts": "on_start",
            "on_successes": "on_success",
            "on_duration_warning_threshold_exceededs": "on_duration_warning_threshold_exceeded",
        }


class JobEnvironmentSpec(BaseModel):
    client: str = Field(..., description="client version used by the environment")
    dependencies: list[str] = Field(
        None,
        description="""
    List of pip dependencies, as supported by the version of pip in this environment. Each dependency is a pip 
    requirement file line. See 
    [API docs](https://docs.databricks.com/api/workspace/jobs/create#environments-spec-dependencies)
    for more information.
    """,
    )


class JobEnvironment(BaseModel):
    environment_key: str = Field(
        ...,
        description="""
        An unique identifier of the Environment. It will be referenced from environment_key attribute of corresponding 
        task.
        """,
    )
    spec: JobEnvironmentSpec = Field(None, description="")


class JobGitSourceGitSnapshot(BaseModel):
    used_commit: str = Field(..., description="")


class JobGitSourceJobSource(BaseModel):
    import_from_git_branch: str = Field(..., description="")
    job_config_path: str = Field(..., description="")
    dirty_state: str = Field(None, description="")


class JobGitSource(BaseModel):
    url: str = Field(..., description="URL of the Git repository to use.")
    branch: str = Field(
        None,
        description="Name of the Git branch to use. Conflicts with `tag` and `commit`.",
    )
    commit: str = Field(
        None,
        description="Hash of Git commit to use. Conflicts with `branch` and `tag`.",
    )
    git_snapshot: JobGitSourceGitSnapshot = Field(
        None, description="Git snapshot specifications"
    )
    job_source: JobGitSourceJobSource = Field(
        None, description="Job source specifications"
    )
    provider: str = Field(
        ...,
        description="""
        Case insensitive name of the Git provider. Following values are supported right now (could be a subject for 
        change, consult 
        [Repos API documentation](https://docs.databricks.com/dev-tools/api/latest/repos.html)):
        `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, 
        `gitLabEnterpriseEdition`.
        """,
    )
    tag: str = Field(
        None,
        description="Name of the Git branch to use. Conflicts with `branch` and `commit`.",
    )


class JobHealthRule(BaseModel):
    metric: str = Field(
        None,
        description="""
    Metric to check. The only supported metric is RUN_DURATION_SECONDS (check Jobs REST API documentation for the latest
    information).
    """,
    )
    op: str = Field(
        None,
        description="""
    Operation used to compare operands. Currently, following operators are supported: EQUAL_TO, GREATER_THAN, 
    GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL.
    """,
    )
    value: int = Field(None, description="Value used to compare to the given metric.")


class JobHealth(BaseModel):
    rules: list[JobHealthRule] = Field(
        None, description="Job health rules specifications"
    )


class JobNotificationSettings(BaseModel):
    no_alert_for_canceled_runs: bool = Field(
        None, description="If `True`, don't send alert for cancelled runs."
    )
    no_alert_for_skipped_runs: bool = Field(
        None, description="If `True`, don't send alert for skipped runs."
    )


class JobParameter(BaseModel):
    default: str = Field(None, description="Default value of the parameter.")
    name: str = Field(
        None,
        description="The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`,",
    )


class JobRunAs(BaseModel):
    service_principal_name: str = Field(
        None,
        description="The application ID of an active service principal. Setting this field requires the servicePrincipal/user role.",
    )
    user_name: str = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email.",
    )


class JobSchedule(BaseModel):
    quartz_cron_expression: str = Field(
        ...,
        description="A Cron expression using Quartz syntax that describes the schedule for a job. This field is required.",
    )
    timezone_id: str = Field(
        ...,
        description="""
    A Java timezone ID. The schedule for a job will be resolved with respect to this timezone. See Java TimeZone for 
    details. This field is required.
    """,
    )
    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str, None] = Field(
        None,
        description="""
        Indicate whether this schedule is paused or not. When the pause_status field is omitted and a schedule is
        provided, the server will default to using `UNPAUSED` as a value for pause_status.
        """,
    )


class JobTaskConditionTask(BaseModel):
    left: str = Field(
        None,
        description="The left operand of the condition task. It could be a string value, job state, or a parameter reference.",
    )
    op: Literal[
        "EQUAL_TO",
        "GREATER_THAN",
        "GREATER_THAN_OR_EQUAL",
        "LESS_THAN",
        "LESS_THAN_OR_EQUAL",
        "NOT_EQUAL",
    ] = Field(
        None,
        description="""
        The string specifying the operation used to compare operands. This task does not require a cluster to execute
        and does not support retries or notifications.
        """,
    )
    right: str = Field(
        None,
        description="The right operand of the condition task. It could be a string value, job state, or parameter reference.",
    )


class JobTaskDependsOn(BaseModel):
    task_key: str = Field(
        None, description="The name of the task this task depends on."
    )
    outcome: Literal["true", "false"] = Field(
        None,
        description="""
    Can only be specified on condition task dependencies. The outcome of the dependent task that must be met for this
    task to run.""",
    )


class JobTaskNotebookTask(BaseModel):
    notebook_path: str = Field(
        ...,
        description="""
    The path of the databricks.Notebook to be run in the Databricks workspace or remote repository. For notebooks
    stored in the Databricks workspace, the path must be absolute and begin with a slash. For notebooks stored in a
    remote repository, the path must be relative.
    """,
    )
    base_parameters: dict[str, Any] = Field(
        None,
        description="""
    Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with
    parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters
    and in run-now, the value from run-now will be used. If the notebook takes a parameter that is not specified
    in the job’s base_parameters or the run-now override parameters, the default value from the notebook will be
    used. Retrieve these parameters in a notebook using dbutils.widgets.get.
    """,
    )
    warehouse_id: str = Field(
        None,
        description="""
    The id of the SQL warehouse to execute this task. If a warehouse_id is specified, that SQL warehouse will be
    used to execute SQL commands inside the specified notebook.
    """,
    )
    source: Literal["WORKSPACE", "GIT"] = Field(
        None,
        description="""
    Location type of the notebook, can only be WORKSPACE or GIT. When set to WORKSPACE, the notebook will be
    retrieved from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git
    repository defined in git_source. If the value is empty, the task will use GIT if git_source is defined and
    WORKSPACE otherwise.
    """,
    )


class JobTaskPipelineTask(BaseModel):
    pipeline_id: str = Field(None, description="The pipeline's unique ID.")
    full_refresh: bool = Field(
        None, description="Specifies if there should be full refresh of the pipeline."
    )


class JobTaskDbtTask(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    commands: list[str] = Field(
        ...,
        description="Series of dbt commands to execute in sequence. Every command must start with 'dbt'.",
    )
    catalog: str = Field(
        None, description="The name of the catalog to use inside Unity Catalog."
    )
    profiles_directory: str = Field(
        None,
        description="""
    The relative path to the directory in the repository specified by `git_source` where dbt should look in for the 
    `profiles.yml` file. If not specified, defaults to the repository's root directory. Equivalent to passing 
    `--profile-dir` to a dbt command.
    """,
    )
    project_directory: str = Field(
        None,
        description="""
    The path where dbt should look for dbt_project.yml. Equivalent to passing `--project-dir` to the dbt CLI.
    - If source is GIT: Relative path to the directory in the repository specified
      in the git_source block. Defaults to the repository's root directory when not
      specified.
    - If source is WORKSPACE: Absolute path to the folder in the workspace.    
    """,
    )
    schema_: str = Field(
        None,
        alias="schema",
        description="The name of the schema dbt should run in. Defaults to `default`.",
    )  # required not to overwrite BaseModel attribute
    source: str = Field(
        None,
        description="""
    The source of the project. Possible values are `WORKSPACE` and `GIT`. Defaults to `GIT` if a `git_source` block is 
    present in the job definition.
    """,
    )
    warehouse_id: str = Field(
        None, description="The ID of the SQL warehouse that dbt should execute against."
    )


class JobTaskRunJobTask(BaseModel):
    job_id: Union[int, str] = Field(None, description="ID of the job")
    job_parameters: dict[str, Any] = Field(
        None, description="Job parameters for the task"
    )


class JobTaskSqlTaskQuery(BaseModel):
    query_id: str = Field(None, description="Query ID")


class JobTaskSqlTaskAlertSubscription(BaseModel):
    destination_id: str = Field(None, description="")
    user_name: str = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email.",
    )


class JobTaskSQLTaskAlert(BaseModel):
    alert_id: str = Field(None, description="Identifier of the Databricks SQL Alert.")
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = Field(
        None,
        description="""
    A list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or 
    `destination_id` - for Alert destination's identifier.
    """,
    )
    pause_subscriptions: bool = Field(
        None, description="It `True` subscriptions are paused"
    )


class JobTaskSqlTaskDashboard(BaseModel):
    dashboard_id: str = Field(
        None,
        description="identifier of the Databricks SQL Dashboard databricks_sql_dashboard.",
    )
    custom_subject: list[JobTaskSqlTaskAlertSubscription] = Field(
        None, description="Custom subject specifications"
    )
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = Field(
        None, description="Subscriptions specifications"
    )


class JobTaskSqlTaskFile(BaseModel):
    path: str = Field(
        None,
        description="""
    If source is `GIT`: Relative path to the file in the repository specified in the git_source block with SQL commands 
    to execute. If source is `WORKSPACE`: Absolute path to the file in the workspace with SQL commands to execute.
    """,
    )
    source: Literal["WORKSPACE", "GIT"] = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`.",
    )


class JobTaskSQLTask(BaseModel):
    alert: JobTaskSQLTaskAlert = Field(None, description="Alert specifications")
    dashboard: JobTaskSqlTaskDashboard = Field(
        None, description="Dashboard specifications"
    )
    file: JobTaskSqlTaskFile = Field(None, description="File specifications")
    parameters: dict[str, Any] = Field(None, description="Parameters specifications")
    query: JobTaskSqlTaskQuery = Field(None, description="Query specifications")
    warehouse_id: str = Field(..., description="Warehouse id")


class JobTaskPythonWheelTask(BaseModel):
    entry_point: str = Field(
        ..., description="Python function as entry point for the task"
    )
    named_parameters: dict[str, str] = Field(
        None, description="Named parameters for the task"
    )
    package_name: str = Field(None, description="Name of Python package")
    parameters: list[str] = Field(None, description="Parameters for the task")


class JobTaskForEachTaskTask(BaseModel):
    # compute_key: str = Field(None, description="")
    dbt_task: JobTaskDbtTask = Field(None, description="dbt task")
    condition_task: JobTaskConditionTask = Field(
        None, description="Condition Task specifications"
    )
    depends_ons: list[JobTaskDependsOn] = Field(
        None, description="Depends On specifications"
    )
    description: str = Field(None, description="Description for this task")
    email_notifications: JobEmailNotifications = Field(
        None, description="Email Notifications specifications"
    )
    environment_key: str = Field(
        None,
        description="""
    Identifier of an `environment` that is used to specify libraries. Required for some tasks (`spark_python_task`, 
    `python_wheel_task`, …) running on serverless compute.
    """,
    )
    existing_cluster_id: str = Field(
        None,
        description="Cluster id from one of the clusters available in the workspace",
    )
    health: JobHealth = Field(None, description="Job Health specifications")
    job_cluster_key: str = Field(
        None,
        description="Identifier that can be referenced in task block, so that cluster is shared between tasks",
    )
    libraries: list[ClusterLibrary] = Field(
        None, description="Cluster Library specifications"
    )
    max_retries: int = Field(
        None,
        description="An optional maximum number of times to retry an unsuccessful run.",
    )
    min_retry_interval_millis: int = Field(
        None,
        description="""
    An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run.
    The default behavior is that unsuccessful runs are immediately retried.
    """,
    )
    # new_cluster: Cluster = Field(None, description="")
    notebook_task: JobTaskNotebookTask = Field(
        None, description="Notebook Task specifications"
    )
    notification_settings: JobNotificationSettings = Field(
        None, description="Notification Settings specifications"
    )
    pipeline_task: JobTaskPipelineTask = Field(
        None, description="Pipeline Task specifications"
    )
    python_wheel_task: JobTaskPythonWheelTask = Field(None, description="")
    retry_on_timeout: bool = Field(
        None,
        description="If `True`, retry a job when it times out. The default behavior is to not retry on timeout.",
    )
    run_if: str = Field(
        None,
        description="""
    An optional value indicating the condition that determines whether the task should be run once its dependencies
    have been completed. When omitted, defaults to `ALL_SUCCESS`.
    """,
    )
    run_job_task: JobTaskRunJobTask = Field(None, description="Run Job specifications")
    # spark_jar_task:
    # spark_python_task:
    sql_task: JobTaskSQLTask = Field(None, description="SQL Task specifications")
    task_key: str = Field(None, description="A unique key for a given task.")
    timeout_seconds: int = Field(
        None,
        description="An optional timeout applied to each run of this job. The default behavior is to have no timeout.",
    )

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "email_notifications": "email_notifications",
        }

    @field_validator("depends_ons")
    @classmethod
    def sort_depends_ons(cls, v: list[JobTaskDependsOn]) -> list[JobTaskDependsOn]:
        return sorted(v, key=lambda task: task.task_key)


class JobTaskForEachTask(BaseModel):
    inputs: str | list = Field(
        ...,
        description="""
    Array for task to iterate on. This can be a JSON string or a reference
    to an array parameter. Laktory also supports a list input, which wil
    be serialized.
    """,
    )
    task: JobTaskForEachTaskTask = Field(
        ..., description="Task to run against the inputs list."
    )
    concurrency: int = Field(
        None,
        description="""
    Controls the number of active iteration task runs. Default is 20, maximum allowed is 100.
    """,
    )

    @field_validator("inputs")
    def parse_inputs(cls, v: Union[str, list]) -> str:
        if isinstance(v, list):
            v = json.dumps(v)

        v = v.replace("'", '"')

        return v


class JobTask(JobTaskForEachTaskTask):
    for_each_task: JobTaskForEachTask = Field(
        None, description="For each task configuration"
    )


class JobTriggerFileArrival(BaseModel):
    url: str = Field(None, description="URL of the job on the given workspace")
    min_time_between_triggers_seconds: int = Field(
        None,
        description="""
    If set, the trigger starts a run only after the specified amount of time passed since the last time the trigger 
    fired. The minimum allowed value is 60 seconds.
    """,
    )
    wait_after_last_change_seconds: int = Field(
        None,
        description="""
    If set, the trigger starts a run only after no file activity has occurred for the specified amount of time.
    This makes it possible to wait for a batch of incoming files to arrive before triggering a run.
    The minimum allowed value is 60 seconds.
    """,
    )


class JobTrigger(BaseModel):
    file_arrival: JobTriggerFileArrival = Field(
        ..., description="File Arrival specifications"
    )
    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str] = Field(
        None,
        description="""
        Indicate whether this trigger is paused or not. When the pause_status field is omitted in the block, the server
        will default to using `UNPAUSED` as a value for pause_status.
        """,
    )


class JobWebhookNotificationsOnDurationWarningThresholdExceeded(BaseModel):
    id: str = Field(None, description="Unique identifier")


class JobWebhookNotificationsOnFailure(BaseModel):
    id: str = Field(None, description="Unique identifier")


class JobWebhookNotificationsOnStart(BaseModel):
    id: str = Field(None, description="Unique identifier")


class JobWebhookNotificationsOnSuccess(BaseModel):
    id: str = Field(None, description="Unique identifier")


class JobWebhookNotifications(BaseModel):
    on_duration_warning_threshold_exceededs: list[
        JobWebhookNotificationsOnDurationWarningThresholdExceeded
    ] = Field(None, description="Warnings threshold exceeded specifications")
    on_failures: list[JobWebhookNotificationsOnFailure] = Field(
        None, description="On failure specifications"
    )
    on_starts: list[JobWebhookNotificationsOnStart] = Field(
        None, description="On starts specifications"
    )
    on_successes: list[JobWebhookNotificationsOnSuccess] = Field(
        None, description="On successes specifications"
    )


class JobQueue(BaseModel):
    enabled: bool = Field(..., description="If true, enable queueing for the job.")


class JobLookup(ResourceLookup):
    id: str = Field(
        serialization_alias="id", description="The id of the databricks job"
    )


class Job(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Job

    Examples
    --------
    ```py
    import io

    from laktory import models

    # Define job
    job_yaml = '''
    name: job-stock-prices
    clusters:
      - name: main
        spark_version: 16.3.x-scala2.12
        node_type_id: Standard_DS3_v2

    tasks:
      - task_key: ingest
        job_cluster_key: main
        notebook_task:
          notebook_path: /jobs/ingest_stock_prices.py
        libraries:
          - pypi:
              package: yfinance

      - task_key: pipeline
        depends_ons:
          - task_key: ingest
        pipeline_task:
          pipeline_id: 74900655-3641-49f1-8323-b8507f0e3e3b

    access_controls:
      - group_name: account users
        permission_level: CAN_VIEW
      - group_name: role-engineers
        permission_level: CAN_MANAGE_RUN
    '''
    job = models.resources.databricks.Job.model_validate_yaml(io.StringIO(job_yaml))

    # Define job with for each task
    job_yaml = '''
    name: job-hello
    tasks:
      - task_key: hello-loop
        for_each_task:
          inputs:
            - id: 1
              name: olivier
            - id: 2
              name: kubic
          task:
            task_key: hello-task
            notebook_task:
              notebook_path: /Workspace/Users/olivier.soucy@okube.ai/hello-world
              base_parameters:
                input: "{{input}}"
    '''
    job = models.resources.databricks.Job.model_validate_yaml(io.StringIO(job_yaml))
    ```

    References
    ----------

    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
    * [Pulumi Databricks Job](https://www.pulumi.com/registry/packages/databricks/api-docs/job/#databricks-job)
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    continuous: JobContinuous = Field(None, description="Continuous specifications")
    control_run_state: bool = Field(
        None,
        description="""
    If `True`, the Databricks provider will stop and start the job as needed to ensure that the active run for the
    job reflects the deployed configuration. For continuous jobs, the provider respects the pause_status by
    stopping the current active run. This flag cannot be set for non-continuous jobs.
    """,
    )
    description: str = Field(
        None,
        description="An optional description for the job. The maximum length is 1024 characters in UTF-8 encoding.",
    )
    email_notifications: JobEmailNotifications = Field(
        None,
        description="""
    An optional set of email addresses notified when runs of this job begins, completes or fails. The default
    behavior is to not send any emails. This field is a block and is documented below.
    """,
    )
    environments: list[JobEnvironment] = Field(
        None, description="List of environments available for the tasks."
    )
    format: str = Field(None, description="")
    git_source: JobGitSource = Field(
        None, description="Specifies a Git repository for task source code."
    )
    health: JobHealth = Field(None, description="Health specifications")
    job_clusters: list[JobJobCluster] = Field(
        [],
        description="""
    A list of job databricks.Cluster specifications that can be shared and reused by tasks of this job. Libraries 
    cannot be declared in a shared job cluster. You must declare dependent libraries in task settings.
    """,
    )

    lookup_existing: JobLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    max_concurrent_runs: int = Field(
        None,
        description="An optional maximum allowed number of concurrent runs of the job. Defaults to 1.",
    )
    max_retries: int = Field(
        None,
        description="""
    An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it
    completes with a FAILED or INTERNAL_ERROR lifecycle state. The value -1 means to retry indefinitely and the
    value 0 means to never retry. The default behavior is to never retry. A run can have the following lifecycle
    state: PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED or INTERNAL_ERROR.
    """,
    )
    min_retry_interval_millis: int = Field(
        None,
        description="""
    An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run.
    The default behavior is that unsuccessful runs are immediately retried.
    """,
    )
    name: str = Field(None, description="Name of the job")
    name_prefix: str = Field(None, description="Prefix added to the job name")
    name_suffix: str = Field(None, description="Suffix added to the job name")
    notification_settings: JobNotificationSettings = Field(
        None, description="Notifications specifications"
    )
    parameters: list[JobParameter] = Field([], description="Parameters specifications")
    queue: JobQueue = Field(None, description="")
    retry_on_timeout: bool = Field(
        None,
        description="""
        An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on
        timeout.
        """,
    )
    run_as: JobRunAs = Field(None, description="Run as specifications")
    schedule: JobSchedule = Field(None, description="Schedule specifications")
    tags: dict[str, Any] = Field({}, description="Tags as key, value pairs")
    tasks: list[JobTask] = Field([], description="Tasks specifications")
    timeout_seconds: int = Field(
        None,
        description="An optional timeout applied to each run of this job. The default behavior is to have no timeout.",
    )
    trigger: JobTrigger = Field(None, description="Trigger specifications")
    webhook_notifications: JobWebhookNotifications = Field(
        None, description="Webhook notifications specifications"
    )

    @field_validator("tasks")
    @classmethod
    def sort_tasks(cls, v: list[JobTask]) -> list[JobTask]:
        return sorted(v, key=lambda task: task.task_key)

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.name = self.name_prefix + self.name
                self.name_prefix = ""
            if self.name_suffix:
                self.name = self.name + self.name_suffix
                self.name_suffix = ""

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    job_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Job"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "name_prefix", "name_suffix"]

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties

        # Rename dbt task schema
        for task in d["tasks"]:
            if "dbt_task" in task:
                if "schema_" in task["dbt_task"]:
                    task["dbt_task"]["schema"] = task["dbt_task"]["schema_"]
                    del task["dbt_task"]["schema_"]

        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "email_notifications": "email_notifications",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_job"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties

        # Rename dbt task schema
        if "tasks" in d:
            for task in d["tasks"]:
                if "dbt_task" in task:
                    if "schema_" in task["dbt_task"]:
                        task["dbt_task"]["schema"] = task["dbt_task"]["schema_"]
                        del task["dbt_task"]["schema_"]

        return d
