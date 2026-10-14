from laktory.models.resources.databricks.credential_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.credential_base import CredentialBase


class Credential(CredentialBase):
    """
    Databricks Credential

    Unity Catalog service credential, used to access external cloud services.

    Examples
    --------
    ```py
    import io

    from laktory import models

    credential_yaml = '''
    name: azure-service-credential
    purpose: SERVICE
    azure_managed_identity:
      access_connector_id: /subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac
    '''
    credential = models.resources.databricks.Credential.model_validate_yaml(
        io.StringIO(credential_yaml)
    )
    ```

    References
    ----------

    * [Databricks Credential](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/credential)
    """
