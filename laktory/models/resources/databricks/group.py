from typing import Union

from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.group_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.group_base import GroupBase
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.pulumiresource import PulumiResource


class GroupLookup(ResourceLookup):
    id: str = Field(
        None, description="Id of the group. Only supported when using Pulumi backend."
    )
    display_name: str = Field(
        None,
        description="Display name of the group. Only support when using Terraform backend",
    )


class Group(GroupBase, PulumiResource):
    """
    Databricks group

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Group(display_name="role-engineers")
    ```
    """

    lookup_existing: GroupLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    member_ids: list[str] = Field(
        [],
        description="A list of all member ids of the group. Can be users, groups or service principals",
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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
