from laktory.models.resources.databricks.serviceprincipalfederationpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipalfederationpolicy_base import (
    ServicePrincipalFederationPolicyBase,
)


class ServicePrincipalFederationPolicy(ServicePrincipalFederationPolicyBase):
    """
    Databricks Service Principal Federation Policy

    Examples
    --------
    ```py
    import io

    from laktory import models

    policy_yaml = '''
    service_principal_id: ${resources.sp-neptune.id}
    policy_id: neptune-ado-deploy
    oidc_policy:
      issuer: https://vstoken.dev.azure.com/00000000-0000-0000-0000-000000000000
      subject: p://MyOrg/MyProject/my-pipeline
      audiences:
      - api://AzureADTokenExchange
    '''
    policy = models.resources.databricks.ServicePrincipalFederationPolicy.model_validate_yaml(
        io.StringIO(policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks Service Principal Federation Policy](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/service_principal_federation_policy)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.policy_id}-{self.service_principal_id}"
