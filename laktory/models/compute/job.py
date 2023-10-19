from typing import Any
from typing import Literal
from pydantic import model_validator
from pydantic import Field
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission
from laktory.models.compute.cluster import Cluster
from laktory.models.compute.cluster import ClusterLibrary


class JobCluster(Cluster):
    is_pinned: bool = Field(None)
    libraries: list[Any] = Field(None)
    permissions: list[Any] = Field(None)

    @model_validator(mode="after")
    def excluded_fields(self) -> Any:

        for f in [
            "is_pinned",
            "libraries",
            "permissions",
        ]:
            if getattr(self, f, None) not in [None, [], {}]:
                raise ValueError(f"Field {f} should be null")

        return self

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks

        d = self.model_dump(exclude_none=True)
        if "name" in d:
            del d["name"]
        return databricks.JobJobClusterArgs(
            job_cluster_key=self.name,
            new_cluster=databricks.JobJobClusterNewClusterArgs(**d)
        )


class JobContinuous(BaseModel):
    pause_status: Literal["PAUSED", "UNPAUSED"] = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobContinuousArgs(**self.model_dump())


class JobEmailNotifications(BaseModel):
    alert_on_last_attempt: bool = None
    no_alert_for_skipped_runs: bool = None
    on_duration_warning_threshold_exceededs: list[str] = None
    on_failures: list[str] = None
    on_starts: list[str] = None
    on_success: list[str] = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobEmailNotificationsArgs(**self.model_dump())


class JobHealthRule(BaseModel):
    metric: str = None
    op: str = None
    value: int = None


class JobHealth(BaseModel):
    rules: list[JobHealthRule] = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobHealthArgs(**self.model_dump())


class JobNotificationSettings(BaseModel):
    no_alert_for_canceled_runs: bool = None
    no_alert_for_skipped_runs: bool = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobNotificationSettingsArgs(**self.model_dump())


class JobParameter(BaseModel):
    default: str = None
    name: str = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobParameterArgs(**self.model_dump())


class JobRunAs(BaseModel):
    service_principal_name: str = None
    user_name: str = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobRunAsArgs(**self.model_dump())


class JobSchedule(BaseModel):
    quartz_cron_expression: str
    timezone_id: str
    pause_status: str

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobScheduleArgs(**self.model_dump())


class JobTaskConditionTask(BaseModel):
    left: str = None
    op: str = None
    right: str = None


class JobTaskDependsOn(BaseModel):
    task_key: str = None
    outcome: str = None


class JobTaskNotebookTask(BaseModel):
    notebook_path: str = None
    base_parameters: dict[str, Any] = None
    source: str = None


class JobTaskPipelineTask(BaseModel):
    pipeline_id: str = None
    full_refresh: bool = None


class JobTaskRunJobTask(BaseModel):
    job_id: str = None
    job_parameters: dict[str, Any]


class JobTaskSqlTaskQuery(BaseModel):
    query_id: str = None


class JobTaskSqlTaskAlertSubscription(BaseModel):
    destination_id: str = None
    user_name: str = None


class JobTaskSQLTaskAlert(BaseModel):
    alert_id: str = None
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = None
    pause_subscriptions: bool = None


class JobTaskSqlTaskDashboard(BaseModel):
    dashboard_id: str = None
    custom_subject: list[JobTaskSqlTaskAlertSubscription] = None
    subscriptions: list[JobTaskSqlTaskAlertSubscription] = None


class JobTaskSqlTaskFile(BaseModel):
    path: str = None


class JobTaskSQLTask(BaseModel):
    alert: JobTaskSQLTaskAlert = None
    dashboard: JobTaskSqlTaskDashboard = None
    file: JobTaskSqlTaskFile = None
    parameters: dict[str, Any] = None
    query: JobTaskSqlTaskQuery
    warehouse_id: str = None


class JobTask(BaseModel):
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


    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobTaskArgs(**self.model_dump())


class JobTriggerFileArrival(BaseModel):
    url: str = None
    min_time_between_triggers_seconds: int = None
    wait_after_last_change_seconds: int = None


class JobTrigger(BaseModel):
    file_arrival: JobTriggerFileArrival
    pause_status: Literal["PAUSED", "UNPAUSED"] = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobTriggerArgs(**self.model_dump())


class JobWebhookNotificationsOnDurationWarningThresholdExceeded(BaseModel):
    id: str = None


class JobWebhookNotificationsOnFailure(BaseModel):
    id: str = None


class JobWebhookNotificationsOnStart(BaseModel):
    id: str = None


class JobWebhookNotificationsOnSuccess(BaseModel):
    id: str = None


class JobWebhookNotifications(BaseModel):
    on_duration_warning_threshold_exceededs: list[JobWebhookNotificationsOnDurationWarningThresholdExceeded] = None
    on_failures: list[JobWebhookNotificationsOnFailure] = None
    on_starts: list[JobWebhookNotificationsOnStart] = None
    on_successes: list[JobWebhookNotificationsOnSuccess] = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.JobWebhookNotificationsArgs(**self.model_dump())


class Job(BaseModel, Resources):
    clusters: list[JobCluster] = []
    continuous: JobContinuous = None
    control_run_state: bool = None
    email_notifications: JobEmailNotifications = None
    format: str = None
    health: JobHealth = None
    max_concurrent_runs: int = None
    max_retries: int = None
    min_retry_interval_millis: int = None
    name: str = None
    notification_settings: JobNotificationSettings = None
    parameters: list[JobParameter] = []
    permissions: list[Permission] = []
    # queue: Optional[JobQueueArgs] = None
    retry_on_timeout: bool = None
    run_as: JobRunAs = None
    schedule: JobSchedule = None
    tags: dict[str, Any] = {}
    tasks: list[JobTask] = []
    timeout_seconds: int = None
    trigger: JobTrigger = None
    webhook_notifications: JobWebhookNotifications = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.job import PulumiJob
        return PulumiJob(name=name, job=self, opts=opts)
