from laktory.models.resources.databricks.shareprovider_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.shareprovider_base import ShareProviderBase


class ShareProvider(ShareProviderBase):
    """
    Databricks Delta Sharing Provider

    A provider is an entity that shares data with a recipient. Declaring one
    is required to consume shares from an open sharing (token based) provider;
    Databricks-to-Databricks sharing creates the provider automatically. Maps
    to the Terraform `databricks_provider` resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    provider_yaml = '''
    name: partner-org
    comment: Market data shared by a partner
    authentication_type: TOKEN
    recipient_profile_str: ${vars.partner_recipient_profile}
    '''
    provider = models.resources.databricks.ShareProvider.model_validate_yaml(
        io.StringIO(provider_yaml)
    )
    ```

    References
    ----------

    * [Databricks Delta Sharing](https://docs.databricks.com/aws/en/delta-sharing/)
    * [Terraform databricks_provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/provider)
    """
