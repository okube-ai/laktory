from pydantic import Field

from laktory.models.resources.databricks.serviceprincipalsecret_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipalsecret_base import (
    ServicePrincipalSecretBase,
)


class ServicePrincipalSecret(ServicePrincipalSecretBase):
    """
    Databricks Service Principal Secret

    OAuth secret of a service principal, used for machine-to-machine authentication. The
    generated secret is available as an output, e.g. `${resources.my-secret.secret}`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    secret_yaml = '''
    service_principal_id: ${resources.sp-neptune.id}
    lifetime: 7776000s
    '''
    secret = models.resources.databricks.ServicePrincipalSecret.model_validate_yaml(
        io.StringIO(secret_yaml)
    )
    ```

    References
    ----------

    * [Databricks Service Principal Secret](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/service_principal_secret)
    """

    service_principal_id: str | None = Field(
        None,
        description="SCIM ID of the databricks_service_principal (not application ID). Set automatically when "
        "declared in the `secrets` of a service principal.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.service_principal_id
