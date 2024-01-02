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
    groups:
        List of the group names that the user should be member of.
    group_ids:
        Dictionary with mapping between group names and group ids
    roles:
        List of roles assigned to the user e.g. ("account_admin")

    Examples
    --------
    ```py
    from laktory import models

    sp = models.ServicePrincipal(
        display_name="neptune",
        application_id="baf147d1-a856-4de0-a570-8a56dbd7e234",
        groups=[
            "role-engineer",
            "role-analyst",
            "domain-finance",
            "domain-engineering",
        ],
        roles=["account_admin"],
    )
    ```
    """

    allow_cluster_create: bool = False
    application_id: str = None
    disable_as_user_deletion: bool = False
    display_name: str
    groups: list[str] = []
    group_ids: dict[str, str]
    roles: list[str] = []

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def all_resources(self) -> list[PulumiResource]:
        res = [
            self,
        ]

        for role in self.roles:
            res += [
                ServicePrincipalRole(
                    resource_name=f"role-{role}-{self.resource_name}",
                    service_principal_id=self.sp.id,
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
        return ["groups", "roles", "group_ids"]
