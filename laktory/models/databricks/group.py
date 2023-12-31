from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class Group(BaseModel, PulumiResource):
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
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Group"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Group

    # TODO:
    # if group.id is None:
    #     self.group = databricks.Group(name, opts=opts, **group.model_pulumi_dump())
    #     group.id = self.group.id
    # else:
    #     self.group = databricks.Group.get(name, id=group.id)
