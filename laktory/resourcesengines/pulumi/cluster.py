import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.cluster import Cluster

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiCluster(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        cluster: Cluster = None,
        opts=None,
    ):
        if name is None:
            name = f"cluster-{cluster.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.cluster = databricks.Cluster(
            f"cluster-{cluster.name}", opts=opts, **cluster.model_pulumi_dump()
        )

        access_controls = []
        for permission in cluster.permissions:
            access_controls += [
                databricks.PermissionsAccessControlArgs(
                    permission_level=permission.permission_level,
                    group_name=permission.group_name,
                    service_principal_name=permission.service_principal_name,
                    user_name=permission.user_name,
                )
            ]

        if access_controls:
            self.permissions = databricks.Permissions(
                f"permissions-cluster-{cluster.name}",
                access_controls=access_controls,
                cluster_id=self.cluster.id,
                opts=opts,
            )
