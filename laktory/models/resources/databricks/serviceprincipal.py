from typing import Union
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.serviceprincipalrole import (
    ServicePrincipalRole,
)
from laktory.models.resources.databricks.groupmember import GroupMember


class ServicePrincipalLookup(ResourceLookup):
    """
    Attributes
    ----------
    application_id:
        ID of the service principal. The service principal must exist before this resource can be retrieved.
    """

    application_id: str = Field(serialization_alias="id")


class ServicePrincipal(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks account service principal

    Attributes
    ----------
    allow_cluster_create:
        When `True`, the group is allowed to have cluster create permissions
    application_id:
        This is the Azure Application ID of the given Azure service principal
        and will be their form of access and identity. On other clouds than
        Azure this value is auto-generated.
    disable_as_user_deletion:
        If `True` user is disabled instead of delete when the resource is deleted
    display_name:
        Display name for the service principal
    group_ids:
        List of the group ids that the user should be member of.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    roles:
        List of roles assigned to the user e.g. ("account_admin")

    Examples
    --------
    ```py
    from laktory import models

    sp = models.resources.databricks.ServicePrincipal(
        display_name="neptune",
        application_id="baf147d1-a856-4de0-a570-8a56dbd7e234",
        group_ids=[
            "${resources.group-role-engineer.id}",
            "${resources.group-role-analyst.id}",
            "${resources.group-domain-finance.id}",
            "${resources.group-domain-engineering.id}",
        ],
        roles=["account_admin"],
    )
    ```
    """

    allow_cluster_create: bool = False
    application_id: str = None
    disable_as_user_deletion: bool = False
    display_name: str
    lookup_existing: ServicePrincipalLookup = Field(None, exclude=True)
    group_ids: list[str] = []
    roles: list[str] = []

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
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

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ServicePrincipal"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
