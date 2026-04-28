from typing import Union

from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.groupmember import GroupMember
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)
from laktory.models.resources.databricks.serviceprincipal_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.serviceprincipal_base import (
    ServicePrincipalBase,
)
from laktory.models.resources.databricks.serviceprincipalrole import (
    ServicePrincipalRole,
)


class ServicePrincipalLookup(ResourceLookup):
    application_id: str = Field(
        serialization_alias="id",
        description="ID of the service principal. The service principal must exist before this resource can be retrieved.",
    )


class ServicePrincipal(ServicePrincipalBase):
    """
    Databricks account service principal

    Examples
    --------
    ```py
    import io

    from laktory import models

    sp_yaml = '''
    display_name: neptune
    application_id: baf147d1-a856-4de0-a570-8a56dbd7e234
    group_ids:
    - ${resources.group-role-engineer.id}
    - ${resources.group-domain-finance.id}
    roles:
    - account_admin
    '''
    sp = models.resources.databricks.ServicePrincipal.model_validate_yaml(
        io.StringIO(sp_yaml)
    )
    ```

    References
    ----------

    * [Databricks Service Principal](https://docs.databricks.com/en/administration-guide/users-groups/service-principals.html)
    """

    lookup_existing: ServicePrincipalLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    group_ids: list[str] = Field(
        [], description="List of the group ids that the user should be member of."
    )
    roles: list[str] = Field(
        [], description="List of roles assigned to the user e.g. ('account_admin')"
    )
    workspace_permission_assignments: list[MwsPermissionAssignment] = Field(
        None, description=""
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def additional_core_resources(self) -> list:
        """
        - service principal roles
        - service principal group members
        """
        resources = []
        for role in self.roles:
            resources += [
                ServicePrincipalRole(
                    service_principal_id=f"${{resources.{self.resource_name}.id}}",
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids", "workspace_permission_assignments"]
