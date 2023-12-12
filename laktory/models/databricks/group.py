from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource


class Group(BaseModel, BaseResource):
    """
    Databricks group

    Attributes
    ----------
    allow_cluster_create:
        When `True`, the group is allowed to have cluster create permissions
    display_name:
        Display name for the group.
    id:
        Id of the group. Generally used when the group is externally managed
        with an identity provider such as Azure AD, Okta or OneLogin.
    workspace_access
        When `True`, the group is allowed to have workspace access

    Examples
    --------
    ```py
    from laktory import models

    d = models.Group(display_name="role-engineers")
    ```
    """

    allow_cluster_create: bool = False
    display_name: str
    id: Union[str, None] = None
    workspace_access: bool = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["id"]

    def deploy_with_pulumi(self, name: str = None, opts=None):
        """
        Deploy group using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiGroup:
            Pulumi group resource
        """
        from laktory.resourcesengines.pulumi.group import PulumiGroup

        return PulumiGroup(name=name, group=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    d = models.Group(display_name="role-engineers")
    print(d)
