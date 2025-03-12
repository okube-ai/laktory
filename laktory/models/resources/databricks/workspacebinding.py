from typing import Literal
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceBinding(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Workspace Binding

    A binding of a workspace to some Databricks resource, such as catalog.

    Attributes
    ----------
    securable_name:
        Name of securable. 
    workspace_id:
        The ID of the workspace to bind the resource to. Changes forces new resource.
    securable_type:
        Type of securable. Can be `catalog`, `external_location`, `storage_credential` or `credential`. Default to `catalog`
    binding_type:
        (Optional) Binding mode. Default to `BINDING_TYPE_READ_WRITE`. Possible values are `BINDING_TYPE_READ_ONLY`, `BINDING_TYPE_READ_WRITE`
    Examples
    --------
    ```py
    ```
    """

    securable_name: str = None
    workspace_id: str
    securable_type: Literal["catalog", "external_location", "storage_credential", "credential"] = "catalog"
    binding_type: Literal["BINDING_TYPE_READ_ONLY", "BINDING_TYPE_READ_WRITE"] = "BINDING_TYPE_READ_WRITE"

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
