from typing import Any
from typing import Literal
from typing import Union
from pydantic import field_validator, model_validator

from pydantic import Field
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.cluster import ClusterLibrary
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class JobCluster(Cluster):
    """
    Job Cluster. Same attributes as `laktory.models.Cluster`, except for

    * `access_controls`
    * `is_pinned`
    * `libraries`
    * `no_wait`

    that are not allowed.
    """

    access_controls: list[Any] = Field(None, exclude=True)
    is_pinned: bool = Field(None, exclude=True)
    libraries: list[Any] = Field(None, exclude=True)
    no_wait: bool = Field(None, exclude=True)

    @model_validator(mode="after")
    def excluded_fields(self) -> Any:
        for f in [
            "access_controls",
            "is_pinned",
            "libraries",
            "no_wait",
        ]:
            if getattr(self, f, None) not in [None, [], {}]:
                raise ValueError(f"Field {f} should be null")

        return self


class JobContinuous(BaseModel):
    """
    Job Continuous specifications

    Attributes
    ----------
    pause_status:
        Indicate whether this continuous job is paused or not. When the pause_status field is omitted in the block,
        the server will default to using `UNPAUSED` as a value for pause_status.
    """

    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str] = None


class JobEmailNotifications(BaseModel):
    """
    Job Email Notifications specifications

    Attributes
    ----------
    no_alert_for_skipped_runs:
        If `True`, don't send alert for skipped runs. (It's recommended to use the corresponding setting in the
        notification_settings configuration block).
    on_duration_warning_threshold_exceededs:
        List of emails to notify when the duration of a run exceeds the threshold specified by the RUN_DURATION_SECONDS
        metric in the health block.
    on_failures:
        List of emails to notify when the run fails.
    on_starts:
        List of emails to notify when the run starts.
    on_successes:
        List of emails to notify when the run completes successfully.
    """

    no_alert_for_skipped_runs: bool = None
    on_duration_warning_threshold_exceededs: list[str] = None
    on_failures: list[str] = None
    on_starts: list[str] = None
    on_successes: list[str] = None

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "on_failures": "on_failure",
            "on_starts": "on_start",
            "on_successes": "on_success",
            "on_duration_warning_threshold_exceededs": "on_duration_warning_threshold_exceeded",
        }


class JobHealthRule(BaseModel):
    """
    Job Health Rule specifications

    Attributes
    ----------
    metric:
        Metric to check. The only supported metric is
        RUN_DURATION_SECONDS (check Jobs REST API documentation for the latest
        information).
    op:
        Operation used to compare operands. Currently, following operators are
        supported: EQUAL_TO, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN,
        LESS_THAN_OR_EQUAL, NOT_EQUAL.
    value:
        Value used to compare to the given metric.
    """

    metric: str = None
    op: str = None
    value: int = None


class JobHealth(BaseModel):
    """
    Job Health specifications

    Attributes
    ----------
    rules:
        Job health rules specifications
    """

    rules: list[JobHealthRule] = None


class JobNotificationSettings(BaseModel):
    """
    Job Notification Settings specifications

    Attributes
    ----------
    no_alert_for_canceled_runs:
        If `True`, don't send alert for cancelled runs.
    no_alert_for_skipped_runs:
        If `True`, don't send alert for skipped runs.
    """

    no_alert_for_canceled_runs: bool = None
    no_alert_for_skipped_runs: bool = None


class JobParameter(BaseModel):
    """
    Job Parameter specifications

    Attributes
    ----------
    default:
        Default value of the parameter.
    name:
        The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`,
    """

    default: str = None
    name: str = None


class JobRunAs(BaseModel):
    """
    Job Parameter specifications

    Attributes
    ----------
    service_principal_name:
        The application ID of an active service principal. Setting this field requires the servicePrincipal/user role.
    user_name:
        The email of an active workspace user. Non-admin users can only set this field to their own email.
    """

    service_principal_name: str = None
    user_name: str = None


class JobSchedule(BaseModel):
    """
    Job Schedule specifications

    Attributes
    ----------
    quartz_cron_expression:
        A Cron expression using Quartz syntax that describes the schedule for a job. This field is required.
    timezone_id:
        A Java timezone ID. The schedule for a job will be resolved with respect to this timezone. See Java TimeZone for
        details. This field is required.
    pause_status:
        Indicate whether this schedule is paused or not. When the pause_status field is omitted and a schedule is
        provided, the server will default to using `UNPAUSED` as a value for pause_status.
    """

    quartz_cron_expression: str
    timezone_id: str
    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str, None] = None


class JobTaskConditionTask(BaseModel):
    """
    Job Task Condition Task specifications

    Attributes
    ----------
    left:
        The left operand of the condition task. It could be a string value, job state, or a parameter reference.
    op:
        The string specifying the operation used to compare operands. This task does not require a cluster to execute
         and does not support retries or notifications.
    right:
        The right operand of the condition task. It could be a string value, job state, or parameter reference.
    """

    left: str = None
    op: Literal[
        "EQUAL_TO",
        "GREATER_THAN",
        "GREATER_THAN_OR_EQUAL",
        "LESS_THAN",
        "LESS_THAN_OR_EQUAL",
        "NOT_EQUAL",
    ] = None
    right: str = None


class JobTaskDependsOn(BaseModel):
    """
    Job Task Depends On specifications

    Attributes
    ----------
    task_key:
        The name of the task this task depends on.
    outcome:
        Can only be specified on condition task dependencies. The outcome of the dependent task that must be met for
        this task to run.
    """

    task_key: str = None
    outcome: Literal["true", "false"] = None


class JobTaskNotebookTask(BaseModel):
    """
    Job Task Notebook Task specifications

    Attributes
    ----------
    notebook_path:
        The path of the databricks.Notebook to be run in the Databricks workspace or remote repository. For notebooks
        stored in the Databricks workspace, the path must be absolute and begin with a slash. For notebooks stored in a
        remote repository, the path must be relative.
    base_parameters:
        Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with
        parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters
        and in run-now, the value from run-now will be used. If the notebook takes a parameter that is not specified
        in the job’s base_parameters or the run-now override parameters, the default value from the notebook will be
        used. Retrieve these parameters in a notebook using dbutils.widgets.get.
    warehouse_id:
        The id of the SQL warehouse to execute this task. If a warehouse_id is specified, that SQL warehouse will be
        used to execute SQL commands inside the specified notebook.
    source:
        Location type of the notebook, can only be WORKSPACE or GIT. When set to WORKSPACE, the notebook will be
        retrieved from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git
        repository defined in git_source. If the value is empty, the task will use GIT if git_source is defined and
        WORKSPACE otherwise.
    """

    notebook_path: str
    base_parameters: dict[str, Any] = None
    warehouse_id: str = None
    source: Literal["WORKSPACE", "GIT"] = None


class JobTaskPipelineTask(BaseModel):
    """
    Job Task Pipeline specifications

    Attributes
    ----------
    pipeline_id:
        The pipeline's unique ID.
    full_refresh:
        Specifies if there should be full refresh of the pipeline.
    """

    pipeline_id: str = None
    full_refresh: bool = None


class JobTaskRunJobTask(BaseModel):
    """
    Job Task Run Job Task specifications

    Attributes
    ----------
    job_id:
        ID of the job
    job_parameters:
        Job parameters for the task
    """

    job_id: Union[int, str] = None
    job_parameters: dict[str, Any] = None


class JobTaskSqlTaskQuery(BaseModel):
    """
    Job Task SQL Task specifications

    Attributes
    ----------
    query_id:
        Query ID
    """

    query_id: str = None


class JobTaskSqlTaskAlertSubscription(BaseModel):
    """
    Job Task SQL Task Alert Subscription specifications

    Attributes
    ----------
    destination_id:

    user_name:
        The email of an active workspace user. Non-admin users can only set this field to their own email.
    """

    destination_id: str = None
    user_name: str = None


class JobTaskSQLTaskAlert(BaseModel):
    """
    Job Task SQL Task Alert specifications

    Attributes
    ----------
    alert_id:
        Identifier of the Databricks SQL Alert.
    subscriptions:
        A list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or
        `destination_id` - for Alert destination's identifier.
    pause_subscriptions:
        It `True` subscriptions are paused
    """

    alert_id: str = None
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = None
    pause_subscriptions: bool = None


class JobTaskSqlTaskDashboard(BaseModel):
    """
    Job Task SQL Task Dashboard specifications

    Attributes
    ----------
    dashboard_id:
        identifier of the Databricks SQL Dashboard databricks_sql_dashboard.
    custom_subject:
        Custom subject specifications
    subscriptions:
        Subscriptions specifications
    """

    dashboard_id: str = None
    custom_subject: list[JobTaskSqlTaskAlertSubscription] = None
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = None


class JobTaskSqlTaskFile(BaseModel):
    """
    Job Task SQL Task File specifications

    Attributes
    ----------
    path:
        If source is `GIT`: Relative path to the file in the repository specified
        in the git_source block with SQL commands to execute. If source is
        `WORKSPACE`: Absolute path to the file in the workspace with SQL
        commands to execute.
    source:
        The source of the project. Possible values are `WORKSPACE` and `GIT`.
    """

    path: str = None
    source: Literal["WORKSPACE", "GIT"] = None


class JobTaskSQLTask(BaseModel):
    """
    Job Task SQL Task specifications

    Attributes
    ----------
    alert:
        Alert specifications
    dashboard:
        Dashboard specifications
    file:
        File specifications
    parameters:
        Parameters specifications
    query:
        Query specifications
    warehouse_id:
        Warehouse id
    """

    alert: JobTaskSQLTaskAlert = None
    dashboard: JobTaskSqlTaskDashboard = None
    file: JobTaskSqlTaskFile = None
    parameters: dict[str, Any] = None
    query: JobTaskSqlTaskQuery = None
    warehouse_id: str


class JobTask(BaseModel):
    """
    Job Task specifications

    Attributes
    ----------
    condition_task:
        Condition Task specifications
    depends_ons:
        Depends On specifications
    description:
        specifications
    email_notifications:
        Email Notifications specifications
    existing_cluster_id:
        Cluster id from one of the clusters available in the workspace
    health:
        Job Health specifications
    job_cluster_key:
        Identifier that can be referenced in task block, so that cluster is shared between tasks
    libraries:
        Cluster Library specifications
    max_retries:
        An optional maximum number of times to retry an unsuccessful run.
    min_retry_interval_millis:
        An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run.
         The default behavior is that unsuccessful runs are immediately retried.
    notebook_task:
        Notebook Task specifications
    notification_settings:
        Notification Settings specifications
    pipeline_task:
        Pipeline Task specifications
    retry_on_timeout:
        If `True`, retry a job when it times out. The default behavior is to not retry on timeout.
    run_if:
        An optional value indicating the condition that determines whether the task should be run once its dependencies
        have been completed. When omitted, defaults to `ALL_SUCCESS`.
    run_job_task:
        Run Job specifications
    sql_task:
        SQL Task specifications
    task_key:
        A unique key for a given task.
    timeout_seconds:
        An optional timeout applied to each run of this job. The default behavior is to have no timeout.
    """

    # compute_key: str = None
    condition_task: JobTaskConditionTask = None
    depends_ons: list[JobTaskDependsOn] = None
    description: str = None
    email_notifications: JobEmailNotifications = None
    existing_cluster_id: str = None
    health: JobHealth = None
    job_cluster_key: str = None
    libraries: list[ClusterLibrary] = None
    max_retries: int = None
    min_retry_interval_millis: int = None
    # new_cluster: Cluster = None
    notebook_task: JobTaskNotebookTask = None
    notification_settings: JobNotificationSettings = None
    pipeline_task: JobTaskPipelineTask = None
    # python_wheel_task:
    retry_on_timeout: bool = None
    run_if: str = None
    run_job_task: JobTaskRunJobTask = None
    # spark_jar_task:
    # spark_python_task:
    sql_task: JobTaskSQLTask = None
    task_key: str = None
    timeout_seconds: int = None

    @field_validator("depends_ons")
    @classmethod
    def sort_depends_ons(cls, v: list[JobTaskDependsOn]) -> list[JobTaskDependsOn]:
        return sorted(v, key=lambda task: task.task_key)


class JobTriggerFileArrival(BaseModel):
    """
    Job Trigger File Arrival

    Attributes
    ----------
    url:
        URL of the job on the given workspace
    min_time_between_triggers_seconds:
        If set, the trigger starts a run only after the specified amount of
        time passed since the last time the trigger fired. The minimum allowed value is 60 seconds.
    wait_after_last_change_seconds:
        If set, the trigger starts a run only after no file activity has occurred for the specified amount of time.
        This makes it possible to wait for a batch of incoming files to arrive before triggering a run.
        The minimum allowed value is 60 seconds.
    """

    url: str = None
    min_time_between_triggers_seconds: int = None
    wait_after_last_change_seconds: int = None


class JobTrigger(BaseModel):
    """
    Job Trigger

    Attributes
    ----------
    file_arrival:
        File Arrival specifications
    pause_status:
        Indicate whether this trigger is paused or not. When the pause_status field is omitted in the block, the server
        will default to using `UNPAUSED` as a value for pause_status.
    """

    file_arrival: JobTriggerFileArrival
    pause_status: Union[Literal["PAUSED", "UNPAUSED"], str] = None


class JobWebhookNotificationsOnDurationWarningThresholdExceeded(BaseModel):
    """
    JobWebhook Notifications On Duration Warning Threshold specifications

    Attributes
    ----------
    id:
        Unique identifier
    """

    id: str = None


class JobWebhookNotificationsOnFailure(BaseModel):
    """
    JobWebhook Notifications On Failure specifications

    Attributes
    ----------
    id:
        Unique identifier
    """

    id: str = None


class JobWebhookNotificationsOnStart(BaseModel):
    """
    JobWebhook Notifications On Start specifications

    Attributes
    ----------
    id:
        Unique identifier
    """

    id: str = None


class JobWebhookNotificationsOnSuccess(BaseModel):
    """
    JobWebhook Notifications On Success specifications

    Attributes
    ----------
    id:
        Unique identifier
    """

    id: str = None


class JobWebhookNotifications(BaseModel):
    """
    Job Webhook Notifications specifications

    Attributes
    ----------
    on_duration_warning_threshold_exceededs:
        Warnings threshold exceeded specifications
    on_failures:
        On failure specifications
    on_starts:
        On starts specifications
    on_successes:
        On successes specifications
    """

    on_duration_warning_threshold_exceededs: list[
        JobWebhookNotificationsOnDurationWarningThresholdExceeded
    ] = None
    on_failures: list[JobWebhookNotificationsOnFailure] = None
    on_starts: list[JobWebhookNotificationsOnStart] = None
    on_successes: list[JobWebhookNotificationsOnSuccess] = None


class JobLookup(ResourceLookup):
    """
    Attributes
    ----------
    id:
        The id of the databricks job
    """

    id: str = Field(serialization_alias="id")


class Job(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Job

    Attributes
    ----------
    access_controls:
        Access Controls specifications
    clusters:
        A list of job databricks.Cluster specifications that can be shared and reused by tasks of this job.
        Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in task settings.
    continuous:
        Continuous specifications
    control_run_state:
        If `True`, the Databricks provider will stop and start the job as needed to ensure that the active run for the
        job reflects the deployed configuration. For continuous jobs, the provider respects the pause_status by
        stopping the current active run. This flag cannot be set for non-continuous jobs.
    description:
        An optional description for the job. The maximum length is 1024 characters in UTF-8 encoding.
    email_notifications:
        An optional set of email addresses notified when runs of this job begins, completes or fails. The default
        behavior is to not send any emails. This field is a block and is documented below.
    format:
    health:
        Health specifications
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    max_concurrent_runs:
        An optional maximum allowed number of concurrent runs of the job. Defaults to 1.
    max_retries:
        An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it
        completes with a FAILED or INTERNAL_ERROR lifecycle state. The value -1 means to retry indefinitely and the
        value 0 means to never retry. The default behavior is to never retry. A run can have the following lifecycle
        state: PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED or INTERNAL_ERROR.
    min_retry_interval_millis:
        An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run.
        The default behavior is that unsuccessful runs are immediately retried.
    name:
        Name of the job
    notification_settings:
        Notifications specifications
    parameters:
        Parameters specifications
    retry_on_timeout:
        An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on
        timeout.
    run_as:
        Run as specifications
    schedule:
        Schedule specifications
    tags:
        Tags as key, value pairs
    tasks:
        Tasks specifications
    timeout_seconds:
        An optional timeout applied to each run of this job. The default behavior is to have no timeout.
    trigger:
        Trigger specifications
    webhook_notifications:
        Webhook notifications specifications

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
        spark_version: 14.0.x-scala2.12
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
    ```

    References
    ----------

    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
    * [Pulumi Databricks Job](https://www.pulumi.com/registry/packages/databricks/api-docs/job/#databricks-job)
    """

    access_controls: list[AccessControl] = []
    clusters: list[JobCluster] = []
    continuous: JobContinuous = None
    control_run_state: bool = None
    description: str = None
    email_notifications: JobEmailNotifications = None
    format: str = None
    health: JobHealth = None
    lookup_existing: JobLookup = Field(None, exclude=True)
    max_concurrent_runs: int = None
    max_retries: int = None
    min_retry_interval_millis: int = None
    name: str = None
    notification_settings: JobNotificationSettings = None
    parameters: list[JobParameter] = []
    # queue: Optional[JobQueueArgs] = None
    retry_on_timeout: bool = None
    run_as: JobRunAs = None
    schedule: JobSchedule = None
    tags: dict[str, Any] = {}
    tasks: list[JobTask] = []
    timeout_seconds: int = None
    trigger: JobTrigger = None
    webhook_notifications: JobWebhookNotifications = None

    @field_validator("tasks")
    @classmethod
    def sort_tasks(cls, v: list[JobTask]) -> list[JobTask]:
        return sorted(v, key=lambda task: task.task_key)

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
        return ["access_controls"]

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"clusters": "job_clusters"}

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties

        _clusters = []
        if settings.camel_serialization:
            k = "jobClusters"
            if k in d:
                for c in d[k]:
                    name = c.pop("name")
                    _clusters += [
                        {
                            "jobClusterKey": name,
                            "newCluster": c,
                        }
                    ]
                d[k] = _clusters

        else:
            k = "job_clusters"
            if k in d:
                for c in d[k]:
                    name = c.pop("name")
                    _clusters += [
                        {
                            "job_cluster_key": name,
                            "new_cluster": c,
                        }
                    ]
                d[k] = _clusters

        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_job"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return {
            "clusters": "job_clusters",
            "cluster": "job_cluster",
        }

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties

        _clusters = []
        k = "job_cluster"
        if k in d:
            for c in d[k]:
                name = c.pop("name")
                _clusters += [
                    {
                        "job_cluster_key": name,
                        "new_cluster": c,
                    }
                ]
            d[k] = _clusters

        return d
