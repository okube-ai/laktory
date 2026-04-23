from pydantic import Field

from laktory.models.resources.databricks.mwspermissionassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwspermissionassignment_base import (
    MwsPermissionAssignmentBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class MwsPermissionAssignment(MwsPermissionAssignmentBase, PulumiResource):
    """
    Databricks Mws Permission Assignment

    Examples
    --------
    ```py
    ```
    """

    principal_id: int = Field(
        None,
        description="Databricks ID of the user, service principal, or group. The principal ID can be retrieved using the SCIM API, or using [databricks_user](../data-sources/user.md), [databricks_service_principal](../data-sources/service_principal.md) or [databricks_group](../data-sources/group.md) data sources",
    )
    workspace_id: int = Field(None, description="Databricks workspace ID")

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
