from laktory.models.resources.databricks.obotoken_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.obotoken_base import OboTokenBase
from laktory.models.resources.pulumiresource import PulumiResource


class OboToken(OboTokenBase, PulumiResource):
    """
    Databricks On-Behalf-Of token
    """

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
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
