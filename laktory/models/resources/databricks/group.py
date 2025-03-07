from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class GroupLookup(ResourceLookup):
    """
    Attributes
    ----------
    id:
        Id of the group. Only supported when using Pulumi backend.
    display_name:
        Display name of the group. Only support when using Terraform backend
    """

    id: str = None
    display_name: str = None


class Group(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks group

    Attributes
    ----------
    acl_principal_id:
        Identifier for use in databricks_access_control_rule_set, e.g. `groups/Some Group`.
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
    display_name:
        Display name for the group.
    external_id:
        ID of the group in an external identity provider.
    force:
        Ignore `cannot create group: Group with name X already exists.` errors and
        implicitly import the specific group into IaC state, enforcing entitlements
        defined in the instance of resource. This functionality is experimental and is
        designed to simplify corner cases, like Azure Active Directory synchronisation.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    workspace_access
        When `True`, the group is allowed to have workspace access

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Group(display_name="role-engineers")
    ```
    """

    acl_principal_id: str = None
    allow_cluster_create: bool = False
    allow_instance_pool_create: bool = None
    databricks_sql_access: bool = None
    display_name: str = None
    external_id: str = None
    force: bool = None
    lookup_existing: GroupLookup = Field(None, exclude=True)
    url: str = None
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
