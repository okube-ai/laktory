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
                f"cluster-{cluster.name}",
                apply_policy_default_values=cluster.apply_policy_default_values,
                autoscale=cluster.autoscale.pulumi_args if cluster.autoscale else None,
                autotermination_minutes=cluster.autotermination_minutes,
                cluster_name=cluster.name,
                custom_tags=cluster.custom_tags,
                data_security_mode=cluster.data_security_mode,
                init_scripts=[i.pulumi_args for i in cluster.init_scripts],
                libraries=[l.pulumi_args for l in cluster.libraries],
                node_type_id=cluster.node_type_id,
                num_workers=cluster.num_workers,
                runtime_engine=cluster.runtime_engine,
                single_user_name=cluster.single_user_name,
                spark_env_vars=cluster.spark_env_vars,
                spark_version=cluster.spark_version,
                is_pinned=cluster.is_pinned,
                opts=opts,
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
