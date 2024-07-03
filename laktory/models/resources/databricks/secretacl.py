from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class SecretAcl(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks secret ACL

    Attributes
    ----------
    permission:
        Scope associated with the secret
    principal:
        Key associated with the secret.
    scope:
        Value associated with the secret
    """

    permission: str = None
    principal: str = None
    scope: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.principal}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SecretAcl"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret_acl"
