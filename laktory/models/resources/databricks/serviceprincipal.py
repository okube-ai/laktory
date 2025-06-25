from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.databricks.serviceprincipalrole import (
    ServicePrincipalRole,
)
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalLookup(ResourceLookup):
    application_id: str = Field(
        serialization_alias="id",
        description="ID of the service principal. The service principal must exist before this resource can be retrieved.",
    )


class ServicePrincipal(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks account service principal

    Examples
    --------
    ```py
    from laktory import models

    sp = models.resources.databricks.ServicePrincipal(
        display_name="neptune",
        application_id="baf147d1-a856-4de0-a570-8a56dbd7e234",
        group_ids=[
            "${resources.group-role-engineer.id}",
            "${resources.group-role-analyst.id}",
            "${resources.group-domain-finance.id}",
            "${resources.group-domain-engineering.id}",
        ],
        roles=["account_admin"],
    )
    ```
    """

    allow_cluster_create: bool = Field(
        False,
        description="When `True`, the group is allowed to have cluster create permissions",
    )
    application_id: str = Field(
        None,
        description="""
    This is the Azure Application ID of the given Azure service principal and will be their form of access and 
    identity. On other clouds than Azure this value is auto-generated.
    """,
    )
    disable_as_user_deletion: bool = Field(
        False,
        description="If `True` user is disabled instead of delete when the resource is deleted",
    )
    display_name: str = Field(..., description="Display name for the service principal")
    lookup_existing: ServicePrincipalLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    group_ids: list[str] = Field(
        [], description="List of the group ids that the user should be member of."
    )
    roles: list[str] = Field(
        [], description="List of roles assigned to the user e.g. ('account_admin')"
    )
    workspace_access: bool = Field(
        None, description="When `True`, the group is allowed to have workspace access"
    )
    workspace_permission_assignments: list[MwsPermissionAssignment] = Field(
        None, description=""
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - service principal roles
        - service principal group members
        """
        resources = []
        for role in self.roles:
            resources += [
                ServicePrincipalRole(
                    service_principal_id=f"${{resources.{self.resource_name}.id}}",
                    role=role,
                )
            ]

        # Group Member
        for group_id in self.group_ids:
            resources += [
                GroupMember(
                    group_id=group_id,
                    member_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        # Workspace Permission Assignments
        if self.workspace_permission_assignments:
            for a in self.workspace_permission_assignments:
                if a.principal_id is None:
                    a.principal_id = f"${{resources.{self.resource_name}.id}}"
                resources += [a]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ServicePrincipal"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids", "workspace_permission_assignments"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
