from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class ServicePrincipal(BaseModel, Resources):
    display_name: str
    application_id: str = None
    allow_cluster_create: bool = False
    groups: list[str] = []
    roles: list[str] = []

    # Deployment options
    disable_as_user_deletion: bool = False

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
