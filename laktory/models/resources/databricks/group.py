from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)


class Group(BaseModel, PulumiResource, TerraformResource):
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

    d = models.resources.databricks.Group(display_name="role-engineers")
    ```
    """

    allow_cluster_create: bool = False
    display_name: str
    id: Union[str, None] = None
    workspace_access: bool = None
    workspace_permission_assignments: list[MwsPermissionAssignment] = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """display name"""
        return self.display_name

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Group"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["workspace_permission_assignments"]

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - workspace permission assignments
        """
        resources = []
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
    def terraform_resource_type(self) -> str:
        return "databricks_group"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
