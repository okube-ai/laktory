from laktory.models.resources.databricks.serviceprincipalrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipalrole_base import (
    ServicePrincipalRoleBase,
)


class ServicePrincipalRole(ServicePrincipalRoleBase):
    """
    Databricks Service Principal Role

    Examples
    --------
    ```py
    import io

    from laktory import models

    role_yaml = '''
    service_principal_id: ${resources.sp-neptune.id}
    role: account_admin
    '''
    role = models.resources.databricks.ServicePrincipalRole.model_validate_yaml(
        io.StringIO(role_yaml)
    )
    ```

    References
    ----------

    * [Databricks Service Principal Role](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/service_principal_role)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.role}-{self.service_principal_id}"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
