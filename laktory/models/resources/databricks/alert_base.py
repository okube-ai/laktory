# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_alert
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AlertConditionOperandColumn(BaseModel):
    name: str = Field(..., description="Name of the column")


class AlertConditionOperand(BaseModel):
    column: AlertConditionOperandColumn | None = Field(
        None,
        description="Block describing the column from the query result to use for comparison in alert evaluation:",
    )


class AlertConditionThresholdValue(BaseModel):
    bool_value: bool | None = Field(
        None,
        description="boolean value (`true` or `false`) to compare against boolean results",
    )
    double_value: float | None = Field(
        None, description="double value to compare against integer and double results"
    )
    string_value: str | None = Field(
        None, description="string value to compare against string results"
    )


class AlertConditionThreshold(BaseModel):
    value: AlertConditionThresholdValue | None = Field(
        None,
        description="actual value used in comparison (one of the attributes is required):",
    )


class AlertCondition(BaseModel):
    empty_result_state: str | None = Field(
        None,
        description="Alert state if the result is empty (`UNKNOWN`, `OK`, `TRIGGERED`)",
    )
    op: str = Field(
        ...,
        description="Operator used for comparison in alert evaluation. (Enum: `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `EQUAL`, `NOT_EQUAL`, `IS_NULL`)",
    )
    operand: AlertConditionOperand | None = Field(
        None,
        description="Name of the column from the query result to use for comparison in alert evaluation:",
    )
    threshold: AlertConditionThreshold | None = Field(
        None, description="Threshold value used for comparison in alert evaluation:"
    )


class AlertBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_alert`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(..., description="Name of the alert")
    query_id: str = Field(..., description="ID of the query evaluated by the alert")
    custom_body: str | None = Field(
        None,
        description="Custom body of alert notification, if it exists. See [Alerts API reference](https://docs.databricks.com/en/sql/user/alerts/index.html) for custom templating instructions",
    )
    custom_subject: str | None = Field(
        None,
        description="Custom subject of alert notification, if it exists. This includes email subject, Slack notification header, etc. See [Alerts API reference](https://docs.databricks.com/en/sql/user/alerts/index.html) for custom templating instructions",
    )
    notify_on_ok: bool | None = Field(
        None,
        description="Whether to notify alert subscribers when alert returns back to normal",
    )
    owner_user_name: str | None = Field(None, description="Alert owner's username")
    seconds_to_retrigger: int | None = Field(
        None,
        description="Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it can be triggered again. If 0 or not specified, the alert will not be triggered again",
    )
    condition: AlertCondition | None = Field(
        None,
        description="Trigger conditions of the alert. Block consists of the following attributes:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_alert"


__all__ = [
    "AlertCondition",
    "AlertConditionOperand",
    "AlertConditionOperandColumn",
    "AlertConditionThreshold",
    "AlertConditionThresholdValue",
    "AlertBase",
]
