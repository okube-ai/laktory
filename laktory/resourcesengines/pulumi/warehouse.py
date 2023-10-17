import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.warehouse import Warehouse

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiWarehouse(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            warehouse: Warehouse = None,
            opts=None,
    ):
        if name is None:
            name = f"warehouse-{warehouse.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.warehouse = databricks.SqlEndpoint(
            f"warehouse-{warehouse.name}",
            auto_stop_mins=warehouse.auto_stop_mins,
            channel=databricks.SqlEndpointChannelArgs(name=warehouse.channel_name),
            cluster_size=warehouse.cluster_size,
            enable_photon=warehouse.enable_photon,
            enable_serverless_compute=warehouse.enable_serverless_compute,
            instance_profile_arn=warehouse.instance_profile_arn,
            jdbc_url=warehouse.jdbc_url,
            max_num_clusters=warehouse.max_num_clusters,
            min_num_clusters=warehouse.min_num_clusters,
            name=warehouse.name,
            num_clusters=warehouse.num_clusters,
            spot_instance_policy=warehouse.spot_instance_policy,
            tags=getattr(warehouse.tags, "pulumi_args", None),
            warehouse_type=warehouse.warehouse_type,
            opts=opts,
        )

        access_controls = []
        for permission in warehouse.permissions:
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
                f"permissions-warehouse-{warehouse.name}",
                access_controls=access_controls,
                sql_endpoint_id=self.warehouse.id,
                opts=opts,
            )
