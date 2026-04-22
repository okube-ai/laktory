from laktory.models.resources.databricks.recipient_base import RecipientBase
from laktory.models.resources.pulumiresource import PulumiResource


class Recipient(RecipientBase, PulumiResource):
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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Recipient"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
