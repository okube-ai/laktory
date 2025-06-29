from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceBinding(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Workspace Binding

    A binding of a workspace to some Databricks resource, such as catalog.


    Examples
    --------
    ```py
    ```
    """

    binding_type: Literal["BINDING_TYPE_READ_ONLY", "BINDING_TYPE_READ_WRITE"] = Field(
        None,
        description="Binding mode. Default to `BINDING_TYPE_READ_WRITE`. Possible values are `BINDING_TYPE_READ_ONLY`, `BINDING_TYPE_READ_WRITE`",
    )
    securable_name: str = Field(None, description="Name of securable.")
    securable_type: Literal[
        "catalog", "external_location", "storage_credential", "credential"
    ] = Field(
        None,
        description="Type of securable. Can be `catalog`, `external_location`, `storage_credential` or `credential`. Default to `catalog`",
    )
    workspace_id: int | str = Field(
        ...,
        description="The ID of the workspace to bind the resource to. Changes forces new resource.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self):
        return f"{self.securable_name}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:WorkspaceBinding"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_binding"
