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
            opts=opts,
            **warehouse.model_pulumi_dump(),
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
