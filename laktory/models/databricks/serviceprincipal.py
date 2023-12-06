from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class ServicePrincipal(BaseModel, Resources):
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
        roles=[
            "account_admin"
        ]
    )
    ```
    """
    allow_cluster_create: bool = False
    application_id: str = None
    disable_as_user_deletion: bool = False
    display_name: str
    groups: list[str] = []
    roles: list[str] = []

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_excludes(self) -> list[str]:
        return ["groups", "roles"]

    def deploy_with_pulumi(self, name=None, group_ids=None, opts=None):
        from laktory.resourcesengines.pulumi.serviceprincipal import (
            PulumiServicePrincipal,
        )

        return PulumiServicePrincipal(
            name=name, service_principal=self, group_ids=group_ids, opts=opts
        )


if __name__ == "__main__":
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
        roles=[
            "account_admin"
        ]
    )
