from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.databricks.sqlquery import SqlQuery

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiSqlQuery(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        sql_query: SqlQuery = None,
        opts=None,
    ):
        if name is None:
            name = sql_query.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            # delete_before_replace=True,
        )

        # As a convenience the data source id is fetch from the warehouse id
        if sql_query.data_source_id is None:
            d = sql_query.inject_vars(sql_query.model_dump())
            warehouse = databricks.SqlEndpoint.get(
                f"warehouse-{name}",
                id=d["warehouse_id"],
                opts=opts,
            )
            sql_query.data_source_id = "${var._data_source_id}"
            sql_query.vars["_data_source_id"] = warehouse.data_source_id

        self.query = databricks.SqlQuery(
            name,
            opts=opts,
            **sql_query.model_pulumi_dump(),
        )

        access_controls = []
        for permission in sql_query.permissions:
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
                f"permissions-{name}",
                access_controls=access_controls,
                sql_query_id=self.query.id,
                opts=opts,
            )
