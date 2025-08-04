from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class NotificationDestinationConfigEmail(BaseModel):
    """Notification Destination Config Email"""

    addresses: list[str] = Field(
        ..., description="The list of email addresses to send notifications to."
    )


class NotificationDestinationConfigGenericWebhook(BaseModel):
    """Notification Destination Config Generic Webhook"""

    password: str = Field(None, description="The password for basic authentication.")
    password_set: str = Field(None, description="")
    url: str = Field(None, description="The Generic Webhook URL.")
    url_set: str = Field(None, description="")
    username: str = Field(None, description="The username for basic authentication.")
    username_set: str = Field(None, description="")


class NotificationDestinationConfigMicrosoftTeams(BaseModel):
    """Notification Destination Config Microsoft Teams"""

    url: str = Field(..., description="The Microsoft Teams webhook URL")
    url_set: str = Field(None, description="")


class NotificationDestinationConfigPagerduty(BaseModel):
    """Notification Destination Config PagerDuty"""

    integration_key: str = Field(..., description="The PagerDuty integration key")
    integration_key_set: str = Field(None, description="")


class NotificationDestinationConfigSlack(BaseModel):
    """Notification Destination Config Slack"""

    url: str = Field(..., description="The Slack webhook URL")
    url_set: str = Field(None, description="")


class NotificationDestinationConfig(BaseModel):
    email: NotificationDestinationConfigEmail = Field(
        None, description="The email configuration of the Notification Destination."
    )
    generic_webhook: NotificationDestinationConfigGenericWebhook = Field(
        None,
        description="The Generic Webhook configuration of the Notification Destination.",
    )
    microsoft_teams: NotificationDestinationConfigMicrosoftTeams = Field(
        None,
        description="The Microsoft Teams configuration of the Notification Destination.",
    )
    pagerduty: NotificationDestinationConfigPagerduty = Field(
        None, description="The PagerDuty configuration of the Notification Destination."
    )
    slack: NotificationDestinationConfigSlack = Field(
        None, description="The Slack configuration of the Notification Destination."
    )

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "microsoft_teams": "microsoft_teams",
        }


class NotificationDestination(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks notification destination

    Examples
    --------
    ```py
    import laktory as lk

    nd_slack = lk.models.resources.databricks.NotificationDestination(
        display_name="slack",
        config={"slack": {"url": "slack.webhook.com"}},
    )
    ```
    """

    display_name: str = Field(
        ..., description="The display name of the Notification Destination."
    )
    config: NotificationDestinationConfig = Field(
        None, description="The configuration of the Notification Destination"
    )
    # destination_type: str = Field(None, description="The type of Notification Destination.")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:NotificationDestination"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_notification_destination"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
