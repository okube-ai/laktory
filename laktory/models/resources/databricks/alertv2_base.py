# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_alert_v2
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AlertV2EvaluationNotificationSubscriptions(BaseModel):
    destination_id: str | None = Field(None)
    user_email: str | None = Field(None)


class AlertV2EvaluationNotification(BaseModel):
    notify_on_ok: bool | None = Field(
        None,
        description="Whether to notify alert subscribers when alert returns back to normal",
    )
    retrigger_seconds: int | None = Field(
        None,
        description="Number of seconds an alert waits after being triggered before it is allowed to send another notification. If set to 0 or omitted, the alert will not send any further notifications after the first trigger Setting this value to 1 allows the alert to send a notification on every evaluation where the condition is met, effectively making it always retrigger for notification purposes * `subscriptions` (list of AlertV2Subscription, optional)",
    )
    subscriptions: list[AlertV2EvaluationNotificationSubscriptions] | None = Field(None)


class AlertV2EvaluationSource(BaseModel):
    aggregation: str | None = Field(
        None,
        description="If not set, the behavior is equivalent to using `First row` in the UI. Possible values are: `AVG`, `COUNT`, `COUNT_DISTINCT`, `MAX`, `MEDIAN`, `MIN`, `STDDEV`, `SUM` * `display` (string, optional)",
    )
    display: str | None = Field(None)
    name: str = Field(
        ...,
        description="The name of the parameter. Reference it in the query text as `:name`. Required, must be non-empty, and must be unique across the alert's parameters",
    )


class AlertV2EvaluationThresholdColumn(BaseModel):
    aggregation: str | None = Field(
        None,
        description="If not set, the behavior is equivalent to using `First row` in the UI. Possible values are: `AVG`, `COUNT`, `COUNT_DISTINCT`, `MAX`, `MEDIAN`, `MIN`, `STDDEV`, `SUM` * `display` (string, optional)",
    )
    display: str | None = Field(None)
    name: str = Field(
        ...,
        description="The name of the parameter. Reference it in the query text as `:name`. Required, must be non-empty, and must be unique across the alert's parameters",
    )


class AlertV2EvaluationThresholdValue(BaseModel):
    bool_value: bool | None = Field(None)
    double_value: float | None = Field(None)
    string_value: str | None = Field(None)


class AlertV2EvaluationThreshold(BaseModel):
    column: AlertV2EvaluationThresholdColumn | None = Field(None)
    value: AlertV2EvaluationThresholdValue | None = Field(
        None,
        description="The value bound to the parameter, represented as a string. If omitted, the value is interpreted as NULL",
    )


class AlertV2Evaluation(BaseModel):
    comparison_operator: str = Field(
        ...,
        description="Operator used for comparison in alert evaluation. Possible values are: `EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `IS_NOT_NULL`, `IS_NULL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `NOT_EQUAL`",
    )
    empty_result_state: str | None = Field(
        None,
        description="Alert state if result is empty. Please avoid setting this field to be `UNKNOWN` because `UNKNOWN` state is planned to be deprecated. Possible values are: `ERROR`, `OK`, `TRIGGERED`, `UNKNOWN`",
    )
    notification: AlertV2EvaluationNotification | None = Field(
        None,
        description="User or Notification Destination to notify when alert is triggered",
    )
    source: AlertV2EvaluationSource | None = Field(
        None, description="Source column from result to use to evaluate alert"
    )
    threshold: AlertV2EvaluationThreshold | None = Field(
        None,
        description="Threshold to user for alert evaluation, can be a column or a value",
    )


class AlertV2Parameters(BaseModel):
    name: str = Field(
        ...,
        description="The name of the parameter. Reference it in the query text as `:name`. Required, must be non-empty, and must be unique across the alert's parameters",
    )
    type: str | None = Field(
        None,
        description="The SQL data type of the parameter, for example `STRING`, `INT`, or `DECIMAL(10, 2)`. If no type is given the type is assumed to be `STRING`. Complex types such as `ARRAY`, `MAP`, and `STRUCT` are not supported",
    )
    value: str | None = Field(
        None,
        description="The value bound to the parameter, represented as a string. If omitted, the value is interpreted as NULL",
    )


class AlertV2ProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AlertV2RunAs(BaseModel):
    service_principal_name: str | None = Field(
        None,
        description="Application ID of an active service principal. Setting this field requires the `servicePrincipal/user` role",
    )
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Can only set this field to their own email",
    )


class AlertV2Schedule(BaseModel):
    pause_status: str | None = Field(
        None,
        description="Indicate whether this schedule is paused or not. Possible values are: `PAUSED`, `UNPAUSED`",
    )
    quartz_cron_schedule: str = Field(
        ...,
        description="A cron expression using quartz syntax that specifies the schedule for this pipeline. Should use the quartz format described here: http://www.quartz-scheduler.org/documentation/quartz-2.1.7/tutorials/tutorial-lesson-06.html",
    )
    timezone_id: str = Field(
        ...,
        description="A Java timezone id. The schedule will be resolved using this timezone. This will be combined with the quartz_cron_schedule to determine the schedule. See https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-conf-mgmt-set-timezone.html for details",
    )


class AlertV2Base(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_alert_v2`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(
        ...,
        description="The display name of the alert * `evaluation` (AlertV2Evaluation, required)",
    )
    query_text: str = Field(
        ...,
        description="Text of the query to be run * `schedule` (CronSchedule, required)",
    )
    warehouse_id: str = Field(
        ..., description="ID of the SQL warehouse attached to the alert"
    )
    custom_description: str | None = Field(
        None, description="Custom description for the alert. support mustache template"
    )
    custom_summary: str | None = Field(
        None, description="Custom summary for the alert. support mustache template"
    )
    purge_on_delete: bool | None = Field(
        None,
        description="Whether to permanently delete the alert. If not set, the alert will only be soft deleted",
    )
    run_as_user_name: str | None = Field(
        None,
        description="The run as username or application ID of service principal. On Create and Update, this field can be set to application ID of an active service principal. Setting this field requires the servicePrincipal/user role. Deprecated: Use `run_as` field instead. This field will be removed in a future release",
    )
    evaluation: AlertV2Evaluation | None = Field(None)
    parameters: list[AlertV2Parameters] | None = Field(
        None,
        description="A list of parameters to pass into the alert SQL query statement containing parameter markers. Static values only",
    )
    provider_config: AlertV2ProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    run_as: AlertV2RunAs | None = Field(
        None,
        description="Specifies the identity that will be used to run the alert. This field allows you to configure alerts to run as a specific user or service principal. - For user identity: Set `user_name` to the email of an active workspace user. Users can only set this to their own email. - For service principal: Set `service_principal_name` to the application ID. Requires the `servicePrincipal/user` role. If not specified, the alert will run as the request user",
    )
    schedule: AlertV2Schedule | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_alert_v2"


__all__ = [
    "AlertV2Base",
    "AlertV2Evaluation",
    "AlertV2EvaluationNotification",
    "AlertV2EvaluationNotificationSubscriptions",
    "AlertV2EvaluationSource",
    "AlertV2EvaluationThreshold",
    "AlertV2EvaluationThresholdColumn",
    "AlertV2EvaluationThresholdValue",
    "AlertV2Parameters",
    "AlertV2ProviderConfig",
    "AlertV2RunAs",
    "AlertV2Schedule",
]
