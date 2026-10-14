# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_budget
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class BudgetAlertConfigurationsActionConfigurations(BaseModel):
    action_configuration_id: str | None = Field(None)
    action_type: str | None = Field(
        None,
        description="The type of action to take when the budget alert is triggered. (Enum: `EMAIL_NOTIFICATION`, `BLOCK_USAGE`). Note: `BLOCK_USAGE` action type is only supported on AI Gateway-scoped budgets",
    )
    target: str | None = Field(
        None,
        description="For `EMAIL_NOTIFICATION` action type, this is the email address to send the notification to. Required if the `action_type` is `EMAIL_NOTIFICATION`. Does not apply to the `BLOCK_USAGE` action type, and this field must be omitted in that case",
    )


class BudgetAlertConfigurationsPrincipalOverrides(BaseModel):
    override_threshold: str | None = Field(
        None,
        description="Dollar amount that overrides the parent alert's `quantity_threshold` for this principal",
    )
    principal_id: int | None = Field(
        None,
        description="Account-level principal id (user, group, or service principal)",
    )


class BudgetAlertConfigurations(BaseModel):
    alert_configuration_id: str | None = Field(None)
    quantity_threshold: str | None = Field(
        None,
        description="The threshold for the budget alert to determine if it is in a triggered state. The number is evaluated based on `quantity_type`",
    )
    quantity_type: str | None = Field(
        None,
        description="The way to calculate cost for this budget alert. This is what quantity_threshold is measured in. (Enum: `LIST_PRICE_DOLLARS_USD`)",
    )
    scope_type: str | None = Field(
        None,
        description="How the alert threshold is evaluated. Determines whether spend is tracked in aggregate or per individual user. (Enum: `ALERT_CONFIGURATION_SCOPE_TYPE_SHARED`, `ALERT_CONFIGURATION_SCOPE_TYPE_PER_USER`)",
    )
    time_period: str | None = Field(
        None,
        description="The time window of usage data for the budget. (Enum: `MONTH`)",
    )
    trigger_type: str | None = Field(
        None,
        description="The evaluation method to determine when this budget alert is in a triggered state. (Enum: `CUMULATIVE_SPENDING_EXCEEDED`)",
    )
    action_configurations: (
        list[BudgetAlertConfigurationsActionConfigurations] | None
    ) = Field(
        None,
        description="List of action configurations to take when the budget alert is triggered. Consists of the following fields:",
    )
    principal_overrides: list[BudgetAlertConfigurationsPrincipalOverrides] | None = (
        Field(
            None,
            description="Per-principal threshold overrides for this alert. Only applies to per-user alerts (`scope_type` = `ALERT_CONFIGURATION_SCOPE_TYPE_PER_USER`); ignored for shared alerts. Consists of the following fields:",
        )
    )


class BudgetFilterTagsValue(BaseModel):
    operator: str | None = Field(
        None, description="The operator to use for the filter. (Enum: `IN`)"
    )
    values: list[str] | None = Field(None, description="The values to filter by")


class BudgetFilterTags(BaseModel):
    key: str | None = Field(None, description="The key of the tag")
    value: BudgetFilterTagsValue | None = Field(
        None, description="Consists of the following fields:"
    )


class BudgetFilterWorkspaceId(BaseModel):
    operator: str | None = Field(
        None, description="The operator to use for the filter. (Enum: `IN`)"
    )
    values: list[int] | None = Field(None, description="The values to filter by")


class BudgetFilter(BaseModel):
    tags: list[BudgetFilterTags] | None = Field(
        None, description="List of tags to filter by. Consists of the following fields:"
    )
    workspace_id: BudgetFilterWorkspaceId | None = Field(
        None,
        description="Filter by workspace ID (if empty, include usage all usage for this account). Consists of the following fields:",
    )


class BudgetBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_budget`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str | None = Field(None, description="The ID of the Databricks Account")
    budget_configuration_id: str | None = Field(
        None, description="The ID of the budget configuration"
    )
    create_time: int | None = Field(None)
    display_name: str | None = Field(
        None, description="Name of the budget in Databricks Account"
    )
    resource_type: str | None = Field(
        None,
        description="The resource scope for this budget. Determines whether the budget tracks all resources or a specific resource. (Enum: `BUDGET_RESOURCE_TYPE_ALL_RESOURCES`, `BUDGET_RESOURCE_TYPE_UNITY_AI_GATEWAY`)",
    )
    update_time: int | None = Field(None)
    alert_configurations: list[BudgetAlertConfigurations] | None = Field(None)
    filter: BudgetFilter | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_budget"


__all__ = [
    "BudgetAlertConfigurations",
    "BudgetAlertConfigurationsActionConfigurations",
    "BudgetAlertConfigurationsPrincipalOverrides",
    "BudgetBase",
    "BudgetFilter",
    "BudgetFilterTags",
    "BudgetFilterTagsValue",
    "BudgetFilterWorkspaceId",
]
