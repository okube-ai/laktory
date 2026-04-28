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
    import io

    from laktory import models

    nd_yaml = '''
    display_name: slack
    config:
      slack:
        url: https://hooks.slack.com/services/T00000000/B00000000/XXXX
    '''
    nd = models.resources.databricks.NotificationDestination.model_validate_yaml(
        io.StringIO(nd_yaml)
    )
    ```

    References
    ----------

    * [Databricks Notification Destination](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notification_destination)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
