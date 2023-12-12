from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource


class ServicePrincipal(BaseModel, BaseResource):
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
        roles=["account_admin"],
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
    def resource_key(self) -> str:
        return self.display_name

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["groups", "roles"]

    def deploy_with_pulumi(
        self, name: str = None, group_ids: dict[str, str] = None, opts=None
    ):
        """
        Deploy service principal using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        group_ids:
            Dictionary whose keys are the display names and whose values are the group ids
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiServicePrincipal:
            Pulumi service principal resource
        """
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
        roles=["account_admin"],
    )
