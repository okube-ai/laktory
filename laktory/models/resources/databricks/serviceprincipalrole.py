from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalRole(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Service Principal role

    Attributes
    ----------
    role:
        This is the id of the role or instance profile resource.
    service_principal_id:
        This is the id of the service principal resource.
    """

    role: str = None
    service_principal_id: str = None

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

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal_role"
