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
    groups:
        List of the group names that the user should be member of.
    group_ids:
        Dictionary with mapping between group names and group ids
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
        groups=[
            "role-engineer",
            "domain-finance",
        ],
        roles=["account_admin"],
    )
    ```
    """

    disable_as_user_deletion: bool = False
    display_name: str = None
    id: Union[str, None] = None
    groups: list[str] = []
    group_ids: dict[str, str]
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
    def all_resources(self) -> list[PulumiResource]:
        res = [
            self,
        ]

        for role in self.roles:
            res += [
                UserRole(
                    resource_name=f"role-{role}-{self.resource_name}",
                    user_id=self.sp.id,
                    role=role,
                )
            ]

        if self.group_ids:

            # Group Member
            for g in self.groups:
                # Find matching group
                group_id = self.group_ids.get(g, None)

                if group_id:
                    res += [
                        GroupMember(
                            resource_name=f"group-member-{self.display_name}-{g}",
                            group_id=group_id,
                            member_id=self.sp.id,
                        )
                    ]

        return res

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "id", "group_ids"]
