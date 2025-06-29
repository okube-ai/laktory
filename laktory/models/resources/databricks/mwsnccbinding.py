from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MwsNccBinding(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Mws Network Connectivity Config Binding

    Examples
    --------
    ```py
    ```
    """

    network_connectivity_config_id: str = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account.",
    )
    workspace_id: str = Field(
        ...,
        description="Identifier of the workspace to attach the NCC to. Change forces creation of a new resource.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self):
        return f"{self.network_connectivity_config_id}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MwsNccBinding"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_ncc_binding"
