from laktory.models.resources.databricks.serviceprincipalrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipalrole_base import (
    ServicePrincipalRoleBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class ServicePrincipalRole(ServicePrincipalRoleBase, PulumiResource):
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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ServicePrincipalRole"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
