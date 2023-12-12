from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource


class User(BaseModel, BaseResource):
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
    roles: list[str] = []
    user_name: str
    workspace_access: bool = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.user_name

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["groups", "roles", "id"]

    def deploy_with_pulumi(self, name=None, group_ids=None, opts=None):
        """
        Deploy user using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        group_ids:
            Dictionary whose keys are the display names and whose values are the group ids
        opts:
            Pulumi resource options

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

        Returns
        -------
        PulumiUser:
            Pulumi user resource
        """
        from laktory.resourcesengines.pulumi.user import PulumiUser

        return PulumiUser(name=name, user=self, group_ids=group_ids, opts=opts)


if __name__ == "__main__":
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
