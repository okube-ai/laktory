from typing import Union
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.userrole import UserRole
from laktory.models.resources.databricks.groupmember import GroupMember


class UserLookup(ResourceLookup):
    """
    Attributes
    ----------
    user_id:
         ID of the user
    """

    user_id: str = Field(serialization_alias="id")


class User(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks user

    Attributes
    ----------
    disable_as_user_deletion:
        If `True` user is disabled instead of delete when the resource is deleted
    display_name:
        Display name for the user
    group_ids:
        List of the group ids that the user should be member of.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    roles:
        List of roles assigned to the user e.g. ("account_admin")
    workspace_access
        When `True`, the user is allowed to have workspace access

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

    disable_as_user_deletion: bool = False
    display_name: str = None
    group_ids: list[str] = []
    lookup_existing: UserLookup = Field(None, exclude=True)
    roles: list[str] = []
    user_name: str
    workspace_access: bool = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.user_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
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

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:User"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_user"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
