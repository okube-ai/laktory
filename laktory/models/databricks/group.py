from typing import Union
from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class Group(BaseModel, Resources):
    """
    Databricks account group

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
    d = models.Group(
        display_name="role-engineers"
    )
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
    def pulumi_excludes(self) -> list[str]:
        return ["id"]

    def deploy_with_pulumi(self, name=None, opts=None):
        """
        Deploy group using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `group-{self.display_name}`
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
    d = models.Group(
        display_name="role-engineers"
    )
    print(d)
