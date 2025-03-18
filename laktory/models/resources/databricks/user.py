from typing import Any, Union

from pydantic import Field, model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.databricks.userrole import UserRole
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class UserLookup(ResourceLookup):
    """
    Attributes
    ----------
    user_id:
        ID of the user
    user_name:
        User name of the user. The user must exist before 
        this resource can be planned. 
        Argument only supported by Terraform IaC backend.
    """

    user_id: Union[int, str] = Field(serialization_alias="id", default=None)
    user_name: str = None

    @model_validator(mode="after")
    def at_least_one(self) -> Any:
        if self.user_id is None and self.user_name is None:
            raise ValueError("At least `user_id` or `user_name` must be set.")

        if not (self.user_id is None or self.user_name is None):
            raise ValueError("Only one of `user_id` or `user_name` must be set.")

        return self

class User(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks user

    Attributes
    ----------
    acl_principal_id:
        Identifier for use in databricks_access_control_rule_set, e.g. `groups/Some Group`.
    active:
        Either user is active or not. True by default, but can be set to false in case
        of user deactivation with preserving user assets.
    allow_cluster_create:
        This is a field to allow the group to have cluster create privileges. More
        fine grained permissions could be assigned with databricks.Permissions and
        cluster_id argument. Everyone without `allow_cluster_create` argument set, but
        with permission to use Cluster Policy would be able to create clusters, but
        within boundaries of that specific policy.
    allow_instance_pool_create:
        This is a field to allow the group to have instance pool create privileges. More
        fine grained permissions could be assigned with databricks.Permissions and
        instance_pool_id argument.
    databricks_sql_access:
        This is a field to allow the group to have access to Databricks SQL feature in
        User Interface and through databricks_sql_endpoint.
    disable_as_user_deletion:
        Deactivate the user when deleting the resource, rather than deleting the user
        entirely. Defaults to true when the provider is configured at the account-level
        and false when configured at the workspace-level. This flag is exclusive to
        force_delete_repos and force_delete_home_dir flags.
    display_name:
        Display name for the user
    external_id:
        ID of the user in an external identity provider.
    force:
        Ignore `cannot create group: User with username X already exists.` errors and
        implicitly import the specific group into IaC state, enforcing entitlements
        defined in the instance of resource. This functionality is experimental and is
        designed to simplify corner cases, like Azure Active Directory synchronisation.
    force_delete_home_dir:
        This flag determines whether the user's home directory is deleted when the user
        is deleted. It will have not impact when in the accounts SCIM API. False by
        default.
    force_delete_repos:
        This flag determines whether the user's repo directory is deleted when the user
        is deleted. It will have no impact when in the accounts SCIM API. False by
        default.
    group_ids:
        List of the group ids that the user should be member of.
    home:
        Home folder of the user, e.g. /Users/mr.foo@example.com.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    repos:
        Personal Repos location of the user, e.g. /Repos/mr.foo@example.com.
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

    acl_principal_id: str = None
    active: bool = None
    allow_cluster_create: bool = None
    allow_instance_pool_create: bool = None
    databricks_sql_access: bool = None
    disable_as_user_deletion: bool = False
    display_name: str = None
    external_id: str = None
    force: bool = None
    force_delete_home_dir: bool = None
    force_delete_repos: bool = None
    group_ids: list[str] = []
    home: str = None
    lookup_existing: UserLookup = Field(None, exclude=True)
    repos: str = None
    roles: list[str] = []
    user_name: str
    workspace_access: bool = None
    workspace_permission_assignments: list[MwsPermissionAssignment] = None

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
        return "databricks:User"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids", "workspace_permission_assignments"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_user"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
