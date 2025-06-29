from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class GroupLookup(ResourceLookup):
    id: str = Field(
        None, description="Id of the group. Only supported when using Pulumi backend."
    )
    display_name: str = Field(
        None,
        description="Display name of the group. Only support when using Terraform backend",
    )


class Group(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks group

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Group(display_name="role-engineers")
    ```
    """

    acl_principal_id: str = Field(
        None,
        description="Identifier for use in databricks_access_control_rule_set, e.g. `groups/Some Group`.",
    )
    allow_cluster_create: bool = Field(
        False,
        description="""
    This is a field to allow the group to have cluster create privileges. More fine grained permissions could be 
    assigned with databricks.Permissions and cluster_id argument. Everyone without `allow_cluster_create` argument set,
    but with permission to use Cluster Policy would be able to create clusters, but within boundaries of that specific 
    policy.
    """,
    )
    allow_instance_pool_create: bool = Field(
        None,
        description="""
    This is a field to allow the group to have instance pool create privileges. More fine grained permissions could 
    be assigned with databricks.Permissions and instance_pool_id argument.
    """,
    )
    databricks_sql_access: bool = Field(
        None,
        description="""
    This is a field to allow the group to have access to Databricks SQL feature in User Interface and through 
    databricks_sql_endpoint.
    """,
    )
    display_name: str = Field(None, description="Display name for the group.")
    external_id: str = Field(
        None, description="ID of the group in an external identity provider."
    )
    force: bool = Field(
        None,
        description="""
    Ignore `cannot create group: Group with name X already exists.` errors and implicitly import the specific group 
    into IaC state, enforcing entitlements defined in the instance of resource. This functionality is experimental and 
    is designed to simplify corner cases, like Azure Active Directory synchronisation.
    """,
    )
    lookup_existing: GroupLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    member_ids: list[str] = Field(
        [],
        description="A list of all member ids of the group. Can be users, groups or service principals",
    )
    url: str = Field(None, description="")
    workspace_access: bool = Field(
        None, description="When `True`, the group is allowed to have workspace access"
    )
    workspace_permission_assignments: list[MwsPermissionAssignment] = Field(
        None, description="Workspace access privileges"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """display name or id"""
        lookup_display_name = (
            self.lookup_existing.display_name if self.lookup_existing else None
        )
        lookup_id = self.lookup_existing.id if self.lookup_existing else None
        return next(
            (
                attr
                for attr in (
                    self.display_name,
                    self.external_id,
                    lookup_display_name,
                    lookup_id,
                )
                if attr is not None
            ),
            "",
        )

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Group"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["member_ids", "workspace_permission_assignments"]

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - workspace permission assignments
        """
        resources = []

        # Group Members
        for member_id in self.member_ids:
            resources += [
                GroupMember(
                    group_id=f"${{resources.{self.resource_name}.id}}",
                    member_id=member_id,
                )
            ]

        if self.workspace_permission_assignments:
            for a in self.workspace_permission_assignments:
                if a.principal_id is None:
                    a.principal_id = f"${{resources.{self.resource_name}.id}}"
                resources += [a]
        return resources

    # TODO:
    # if group.id is None:
    #     self.group = databricks.Group(name, opts=opts, **group.model_pulumi_dump())
    #     group.id = self.group.id
    # else:
    #     self.group = databricks.Group.get(name, id=group.id)

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_group"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
