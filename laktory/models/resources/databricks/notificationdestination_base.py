# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_notification_destination
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class NotificationDestinationConfigEmail(BaseModel):
    addresses: list[str] | None = Field(
        None, description="The list of email addresses to send notifications to"
    )


class NotificationDestinationConfigGenericWebhook(BaseModel):
    password: str | None = Field(
        None, description="The password for basic authentication"
    )
    password_set: bool | None = Field(None)
    url: str | None = Field(None, description="The Generic Webhook URL")
    url_set: bool | None = Field(None)
    username: str | None = Field(
        None, description="The username for basic authentication"
    )
    username_set: bool | None = Field(None)


class NotificationDestinationConfigMicrosoftTeams(BaseModel):
    app_id: str | None = Field(None, description="App ID for Microsoft Teams App")
    app_id_set: bool | None = Field(None)
    auth_secret: str | None = Field(
        None, description="Secret for Microsoft Teams App authentication"
    )
    auth_secret_set: bool | None = Field(None)
    channel_url: str | None = Field(
        None, description="Channel URL for Microsoft Teams App"
    )
    channel_url_set: bool | None = Field(None)
    tenant_id: str | None = Field(None, description="Tenant ID for Microsoft Teams App")
    tenant_id_set: bool | None = Field(None)
    url: str | None = Field(None, description="The Generic Webhook URL")
    url_set: bool | None = Field(None)


class NotificationDestinationConfigPagerduty(BaseModel):
    integration_key: str | None = Field(
        None, description="The PagerDuty integration key"
    )
    integration_key_set: bool | None = Field(None)


class NotificationDestinationConfigSlack(BaseModel):
    channel_id: str | None = Field(
        None, description="Slack channel ID for notifications"
    )
    channel_id_set: bool | None = Field(None)
    oauth_token: str | None = Field(
        None, description="OAuth token for Slack authentication"
    )
    oauth_token_set: bool | None = Field(None)
    url: str | None = Field(None, description="The Generic Webhook URL")
    url_set: bool | None = Field(None)


class NotificationDestinationConfig(BaseModel):
    email: NotificationDestinationConfigEmail | None = Field(
        None,
        description="The email configuration of the Notification Destination. It must contain the following:",
    )
    generic_webhook: NotificationDestinationConfigGenericWebhook | None = Field(
        None,
        description="The Generic Webhook configuration of the Notification Destination. It must contain the following:",
    )
    microsoft_teams: NotificationDestinationConfigMicrosoftTeams | None = Field(
        None,
        description="The Microsoft Teams configuration of the Notification Destination. It must contain the following:",
    )
    pagerduty: NotificationDestinationConfigPagerduty | None = Field(
        None,
        description="The PagerDuty configuration of the Notification Destination. It must contain the following:",
    )
    slack: NotificationDestinationConfigSlack | None = Field(
        None,
        description="The Slack configuration of the Notification Destination. It must contain the following:",
    )


class NotificationDestinationBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_notification_destination`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(
        ..., description="The display name of the Notification Destination"
    )
    destination_type: str | None = Field(
        None, description="the type of Notification Destination"
    )
    config: NotificationDestinationConfig | None = Field(
        None,
        description="The configuration of the Notification Destination. It must contain exactly one of the following blocks:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_notification_destination"
