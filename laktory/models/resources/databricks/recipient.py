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
    import io

    from laktory import models

    recipient_yaml = '''
    name: partner-org
    authentication_type: TOKEN
    comment: External partner for data sharing
    '''
    recipient = models.resources.databricks.Recipient.model_validate_yaml(
        io.StringIO(recipient_yaml)
    )
    ```

    References
    ----------

    * [Databricks Delta Sharing Recipient](https://docs.databricks.com/en/data-sharing/create-recipient.html)
    """

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
