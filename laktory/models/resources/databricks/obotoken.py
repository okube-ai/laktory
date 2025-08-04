from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class OboToken(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks On-Behalf-Of token
    """

    application_id: str = Field(
        None,
        description="Application ID of databricks.ServicePrincipal to create a PAT token for.",
    )
    comment: str = Field(
        None, description="Comment that describes the purpose of the token."
    )
    lifetime_seconds: str = Field(
        None,
        description="The number of seconds before the token expires. Token resource is re-created when it expires. If no lifetime is specified, the token remains valid indefinitely.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
    #
    @property
    def resource_key(self) -> str:
        return self.application_id

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:OboToken"

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"value": "string_value"}

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_obo_token"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
