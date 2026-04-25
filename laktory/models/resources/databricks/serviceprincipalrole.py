from laktory.models.resources.databricks.serviceprincipalrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipalrole_base import (
    ServicePrincipalRoleBase,
)


class ServicePrincipalRole(ServicePrincipalRoleBase):
    """
    Databricks Service Principal role
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
