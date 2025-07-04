from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Secret(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks secret
    """

    scope: str = Field(None, description="Scope associated with the secret")
    key: str = Field(None, description="Key associated with the secret.")
    value: str = Field(None, description="Value associated with the secret")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.key}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Secret"

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"value": "string_value"}

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
