# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_account_setting_v2
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AccountSettingV2AibiDashboardEmbeddingAccessPolicy(BaseModel):
    access_policy_type: str = Field(
        ...,
        description="Possible values are: `ALLOW_ALL_DOMAINS`, `ALLOW_APPROVED_DOMAINS`, `DENY_ALL_DOMAINS`",
    )


class AccountSettingV2AibiDashboardEmbeddingApprovedDomains(BaseModel):
    approved_domains: list[str] | None = Field(None)


class AccountSettingV2AllowedAppsUserApiScopes(BaseModel):
    allowed_scopes: list[str] | None = Field(None)


class AccountSettingV2AutomaticClusterUpdateWorkspaceEnablementDetails(BaseModel):
    forced_for_compliance_mode: bool | None = Field(
        None, description="The feature is force enabled if compliance mode is active"
    )
    unavailable_for_disabled_entitlement: bool | None = Field(
        None,
        description="The feature is unavailable if the corresponding entitlement disabled (see getShieldEntitlementEnable)",
    )
    unavailable_for_non_enterprise_tier: bool | None = Field(
        None,
        description="The feature is unavailable if the customer doesn't have enterprise tier",
    )


class AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedScheduleWindowStartTime(
    BaseModel
):
    hours: int | None = Field(None)
    minutes: int | None = Field(None)


class AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedSchedule(
    BaseModel
):
    day_of_week: str | None = Field(
        None,
        description="Possible values are: `FRIDAY`, `MONDAY`, `SATURDAY`, `SUNDAY`, `THURSDAY`, `TUESDAY`, `WEDNESDAY`",
    )
    frequency: str | None = Field(
        None,
        description="Possible values are: `EVERY_WEEK`, `FIRST_AND_THIRD_OF_MONTH`, `FIRST_OF_MONTH`, `FOURTH_OF_MONTH`, `SECOND_AND_FOURTH_OF_MONTH`, `SECOND_OF_MONTH`, `THIRD_OF_MONTH` * `window_start_time` (ClusterAutoRestartMessageMaintenanceWindowWindowStartTime, optional)",
    )
    window_start_time: (
        AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedScheduleWindowStartTime
        | None
    ) = Field(None)


class AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindow(BaseModel):
    week_day_based_schedule: (
        AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedSchedule
        | None
    ) = Field(None)


class AccountSettingV2AutomaticClusterUpdateWorkspace(BaseModel):
    can_toggle: bool | None = Field(None)
    enabled: bool | None = Field(None)
    enablement_details: (
        AccountSettingV2AutomaticClusterUpdateWorkspaceEnablementDetails | None
    ) = Field(None)
    maintenance_window: (
        AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindow | None
    ) = Field(None)
    restart_even_if_no_updates_available: bool | None = Field(None)


class AccountSettingV2BooleanVal(BaseModel):
    value: bool | None = Field(None, description="Represents a generic string value")


class AccountSettingV2CollaborationPlatformConnectivity(BaseModel):
    connectivity: str = Field(
        ...,
        description="Possible values are: `ALLOW_ALL`, `ALLOW_SLACK`, `ALLOW_TEAMS`, `DENY_ALL`",
    )


class AccountSettingV2IntegerVal(BaseModel):
    value: int | None = Field(None, description="Represents a generic string value")


class AccountSettingV2OperationalEmailCustomRecipient(BaseModel):
    email: str | None = Field(None)


class AccountSettingV2PersonalCompute(BaseModel):
    value: str | None = Field(None, description="Represents a generic string value")


class AccountSettingV2RestrictWorkspaceAdmins(BaseModel):
    disable_gov_tag_creation: bool | None = Field(
        None,
        description="When true, workspace admins cannot create governance tags. ALLOW_ALL status does not override this; they are independent",
    )
    status: str = Field(
        ...,
        description="Possible values are: `ALLOW_ALL`, `RESTRICT_TOKENS_AND_JOB_RUN_AS`",
    )


class AccountSettingV2StringVal(BaseModel):
    value: str | None = Field(None, description="Represents a generic string value")


class AccountSettingV2WorkspaceLabel(BaseModel):
    color: str | None = Field(None)
    label: str | None = Field(None)


class AccountSettingV2Base(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_account_setting_v2`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str | None = Field(None, description="Name of the setting")
    aibi_dashboard_embedding_access_policy: (
        AccountSettingV2AibiDashboardEmbeddingAccessPolicy | None
    ) = Field(
        None,
        description="Setting value for aibi_dashboard_embedding_access_policy setting. This is the setting value set by consumers, check effective_aibi_dashboard_embedding_access_policy for final setting value",
    )
    aibi_dashboard_embedding_approved_domains: (
        AccountSettingV2AibiDashboardEmbeddingApprovedDomains | None
    ) = Field(
        None,
        description="Setting value for aibi_dashboard_embedding_approved_domains setting. This is the setting value set by consumers, check effective_aibi_dashboard_embedding_approved_domains for final setting value",
    )
    allowed_apps_user_api_scopes: AccountSettingV2AllowedAppsUserApiScopes | None = (
        Field(
            None,
            description="Setting value for allowed_apps_user_api_scopes setting. This is the setting value set by consumers, check effective_allowed_apps_user_api_scopes for final setting value",
        )
    )
    automatic_cluster_update_workspace: (
        AccountSettingV2AutomaticClusterUpdateWorkspace | None
    ) = Field(
        None,
        description="Setting value for automatic_cluster_update_workspace setting. This is the setting value set by consumers, check effective_automatic_cluster_update_workspace for final setting value",
    )
    boolean_val: AccountSettingV2BooleanVal | None = Field(
        None,
        description="Setting value for boolean type setting. This is the setting value set by consumers, check effective_boolean_val for final setting value",
    )
    collaboration_platform_connectivity: (
        AccountSettingV2CollaborationPlatformConnectivity | None
    ) = Field(
        None,
        description="Setting value for collaboration_platform_connectivity setting. This is the setting value set by consumers, check effective_collaboration_platform_connectivity for final setting value",
    )
    integer_val: AccountSettingV2IntegerVal | None = Field(
        None,
        description="Setting value for integer type setting. This is the setting value set by consumers, check effective_integer_val for final setting value",
    )
    operational_email_custom_recipient: (
        AccountSettingV2OperationalEmailCustomRecipient | None
    ) = Field(
        None,
        description="Setting value for operational_email_custom_recipient setting. This is the setting value set by consumers, check effective_operational_email_custom_recipient for final setting value",
    )
    personal_compute: AccountSettingV2PersonalCompute | None = Field(
        None,
        description="Setting value for personal_compute setting. This is the setting value set by consumers, check effective_personal_compute for final setting value",
    )
    restrict_workspace_admins: AccountSettingV2RestrictWorkspaceAdmins | None = Field(
        None,
        description="Setting value for restrict_workspace_admins setting. This is the setting value set by consumers, check effective_restrict_workspace_admins for final setting value",
    )
    string_val: AccountSettingV2StringVal | None = Field(
        None,
        description="Setting value for string type setting. This is the setting value set by consumers, check effective_string_val for final setting value",
    )
    workspace_label: AccountSettingV2WorkspaceLabel | None = Field(
        None,
        description="Setting value for workspace_label setting. This is the setting value set by consumers, check effective_workspace_label for final setting value",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_account_setting_v2"


__all__ = [
    "AccountSettingV2AibiDashboardEmbeddingAccessPolicy",
    "AccountSettingV2AibiDashboardEmbeddingApprovedDomains",
    "AccountSettingV2AllowedAppsUserApiScopes",
    "AccountSettingV2AutomaticClusterUpdateWorkspace",
    "AccountSettingV2AutomaticClusterUpdateWorkspaceEnablementDetails",
    "AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindow",
    "AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedSchedule",
    "AccountSettingV2AutomaticClusterUpdateWorkspaceMaintenanceWindowWeekDayBasedScheduleWindowStartTime",
    "AccountSettingV2Base",
    "AccountSettingV2BooleanVal",
    "AccountSettingV2CollaborationPlatformConnectivity",
    "AccountSettingV2IntegerVal",
    "AccountSettingV2OperationalEmailCustomRecipient",
    "AccountSettingV2PersonalCompute",
    "AccountSettingV2RestrictWorkspaceAdmins",
    "AccountSettingV2StringVal",
    "AccountSettingV2WorkspaceLabel",
]
