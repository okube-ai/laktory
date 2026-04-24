from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.databricks.user_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.user_base import UserBase
from laktory.models.resources.databricks.userrole import UserRole


class UserLookup(ResourceLookup):
    user_id: Union[int, str] = Field(
        serialization_alias="id", default=None, description="ID of the user"
    )
    user_name: str = Field(
        None,
        description="""
    User name of the user. The user must exist before this resource can be planned.
    Argument only supported by Terraform IaC backend.
    """,
    )

    @model_validator(mode="after")
    def at_least_one(self) -> Any:
        if self.user_id is None and self.user_name is None:
            raise ValueError("At least `user_id` or `user_name` must be set.")

        if not (self.user_id is None or self.user_name is None):
            raise ValueError("Only one of `user_id` or `user_name` must be set.")

        return self


class User(UserBase):
    """
    Databricks user

    Examples
    --------
    ```py
    from laktory import models

    u = models.resources.databricks.User(
        user_name="john.doe@okube.ai",
        display_name="John Doe",
        group_ids=[
            "${resources.group-role-engineer.id}",
            "${resources.group-domain-finance.id}",
        ],
        roles=["account_admin"],
    )
    ```
    """

    group_ids: list[str] = Field(
        [], description="List of the group ids that the user should be member of."
    )
    lookup_existing: UserLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    roles: list[str] = Field(
        [], description="List of roles assigned to the user e.g. ('account_admin')"
    )
    workspace_permission_assignments: list[MwsPermissionAssignment] = Field(
        None, description=""
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.user_name

    @property
    def additional_core_resources(self) -> list:
        """
        - user roles
        - user group members
        """
        resources = []
        for role in self.roles:
            resources += [
                UserRole(
                    user_id=f"${{resources.{self.resource_name}.id}}",
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids", "workspace_permission_assignments"]
