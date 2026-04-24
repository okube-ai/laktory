from laktory.models.resources.databricks.notificationdestination_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.notificationdestination_base import (
    NotificationDestinationBase,
)


class NotificationDestination(NotificationDestinationBase):
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
