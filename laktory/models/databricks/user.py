from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.userrole import UserRole
from laktory.models.databricks.groupmember import GroupMember


class User(BaseModel, PulumiResource):
    """
    Databricks user

    Attributes
    ----------
    disable_as_user_deletion:
        If `True` user is disabled instead of delete when the resource is deleted
    display_name:
        Display name for the user
    id:
        Id of the user. Generally used when the user is externally managed
        with an identity provider such as Azure AD, Okta or OneLogin.
    group_ids:
        List of the group ids that the user should be member of.
    roles:
        List of roles assigned to the user e.g. ("account_admin")
    workspace_access
        When `True`, the user is allowed to have workspace access

    Examples
    --------
    ```py
    from laktory import models

    u = models.User(
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
    id: Union[str, None] = None
    group_ids: list[str] = []
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
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [
                self,
            ]

            for role in self.roles:
                self.resources_ += [
                    UserRole(
                        resource_name=f"role-{role}-{self.resource_name}",
                        # user_id=self.sp.id,
                        user_id=f"${{resources.{self.resource_name}.id}}",
                        role=role,
                    )
                ]

            # Group Member
            for group_id in self.group_ids:
                self.resources_ += [
                    GroupMember(
                        resource_name=f"group-member-{self.display_name}-{group_id}",
                        group_id=group_id,
                        member_id=f"${{resources.{self.resource_name}.id}}",
                    )
                ]

        return self.resources_

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:User"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.User

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "id", "group_ids"]
