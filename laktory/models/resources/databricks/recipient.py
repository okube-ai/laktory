from laktory.models.resources.databricks.recipient_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.recipient_base import RecipientBase


class Recipient(RecipientBase):
    """
    Databricks Recipient for Delta Sharing

    A recipient is an entity that can receive shared data from a Databricks workspace.
    Recipients can be configured with authentication details to enable Delta Sharing.

    Examples
    --------
    ```py
    ```
    """

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
