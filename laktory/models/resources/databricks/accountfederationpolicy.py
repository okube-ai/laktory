from laktory.models.resources.databricks.accountfederationpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accountfederationpolicy_base import (
    AccountFederationPolicyBase,
)


class AccountFederationPolicy(AccountFederationPolicyBase):
    """
    Databricks Account Federation Policy

    Examples
    --------
    ```py
    import io

    from laktory import models

    policy_yaml = '''
    policy_id: my-policy
    oidc_policy:
      issuer: https://myidp.example.com
      audiences:
      - api://AzureADTokenExchange
    '''
    policy = models.resources.databricks.AccountFederationPolicy.model_validate_yaml(
        io.StringIO(policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks Account Federation Policy](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/account_federation_policy)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.policy_id
