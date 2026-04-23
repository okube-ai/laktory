from typing import Union

from laktory.models.resources.databricks.notificationdestination_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.notificationdestination_base import (
    NotificationDestinationBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class NotificationDestination(NotificationDestinationBase, PulumiResource):
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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
