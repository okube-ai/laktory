from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.serviceprincipalrole import ServicePrincipalRole
from laktory.models.databricks.groupmember import GroupMember


class ServicePrincipal(BaseModel, PulumiResource):
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
    roles:
        List of roles assigned to the user e.g. ("account_admin")

    Examples
    --------
    ```py
    from laktory import models

    sp = models.ServicePrincipal(
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
    group_ids: list[str] = []
    roles: list[str] = []

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ServicePrincipal"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.ServicePrincipal

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [
                self,
            ]

            for role in self.roles:
                self.resources_ += [
                    ServicePrincipalRole(
                        resource_name=f"role-{role}-{self.resource_name}",
                        service_principal_id=f"${{resources.{self.resource_name}.id}}",
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
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["groups", "roles", "group_ids"]
