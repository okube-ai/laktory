from typing import Literal
from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MwsPermissionAssignment(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Mws Permission Assignment

    Examples
    --------
    ```py
    ```
    """

    permissions: list[Literal["USER", "ADMIN"]] = Field(
        ["USER"],
        description="""
    The list of workspace permissions to assign to the principal:
    - "USER" Can access the workspace with basic privileges.
    - "ADMIN" Can access the workspace and has workspace admin
      privileges to manage users and groups, workspace configurations,
      and more.    
    """,
    )  # required
    principal_id: Union[int, str] = Field(
        None,
        description="""
    Databricks ID of the user, service principal, or group. The principal ID can be retrieved using the SCIM API, or 
    using databricks_user, databricks.ServicePrincipal or databricks.Group data sources.    
    """,
    )  # required
    workspace_id: Union[int, str] = Field(
        None, description="Databricks workspace ID."
    )  # required

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self):
        return "permission"

    @property
    def resource_key(self):
        return f"{self.principal_id}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MwsPermissionAssignment"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_permission_assignment"
