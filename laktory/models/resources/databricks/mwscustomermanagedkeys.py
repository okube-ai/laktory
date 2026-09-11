from laktory.models.resources.databricks.mwscustomermanagedkeys_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwscustomermanagedkeys_base import (
    MwsCustomerManagedKeysBase,
)


class MwsCustomerManagedKeys(MwsCustomerManagedKeysBase):
    """
    Databricks Mws Customer Managed Keys

    Examples
    --------
    ```py
    import io

    from laktory import models

    key_yaml = '''
    account_id: ${vars.databricks_account_id}
    use_cases:
    - MANAGED_SERVICES
    aws_key_info:
      key_arn: arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000
    '''
    key = models.resources.databricks.MwsCustomerManagedKeys.model_validate_yaml(
        io.StringIO(key_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Customer Managed Keys](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_customer_managed_keys)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return "-".join(self.use_cases)
