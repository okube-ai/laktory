# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_sql_endpoint
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SqlEndpointChannel(BaseModel):
    dbsql_version: str | None = Field(None)
    name: str | None = Field(
        None,
        description="Name of the Databricks SQL release channel. Possible values are: `CHANNEL_NAME_PREVIEW` and `CHANNEL_NAME_CURRENT`. Default is `CHANNEL_NAME_CURRENT`",
    )


class SqlEndpointTagsCustomTags(BaseModel):
    key: str = Field(...)
    value: str = Field(...)


class SqlEndpointTags(BaseModel):
    custom_tags: list[SqlEndpointTagsCustomTags] | None = Field(None)


class SqlEndpointTimeouts(BaseModel):
    create: str | None = Field(None)


class SqlEndpointBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_sql_endpoint`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    cluster_size: str = Field(
        ...,
        description="The size of the clusters allocated to the endpoint: '2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large', '5X-Large'",
    )
    name: str = Field(
        ...,
        description="Name of the Databricks SQL release channel. Possible values are: `CHANNEL_NAME_PREVIEW` and `CHANNEL_NAME_CURRENT`. Default is `CHANNEL_NAME_CURRENT`",
    )
    auto_stop_mins: int | None = Field(
        None,
        description="Time in minutes until an idle SQL warehouse terminates all clusters and stops. This field is optional. The default is 120, set to 0 to disable the auto stop",
    )
    data_source_id: str | None = Field(
        None,
        description="(Deprecated, will be removed) ID of the data source for this endpoint. This is used to bind an Databricks SQL query to an endpoint",
    )
    enable_photon: bool | None = Field(
        None,
        description="Whether to enable [Photon](https://databricks.com/product/delta-engine). This field is optional and is enabled by default",
    )
    enable_serverless_compute: bool | None = Field(
        None,
        description="Whether this SQL warehouse is a serverless endpoint. See below for details about the default values. To avoid ambiguity, especially for organizations with many workspaces, Databricks recommends that you always set this field explicitly",
    )
    instance_profile_arn: str | None = Field(None)
    max_num_clusters: int | None = Field(
        None,
        description="Maximum number of clusters available when a SQL warehouse is running. This field is required. If multi-cluster load balancing is not enabled, this is default to `1`",
    )
    min_num_clusters: int | None = Field(
        None,
        description="Minimum number of clusters available when a SQL warehouse is running. The default is `1`",
    )
    no_wait: bool | None = Field(
        None,
        description="Whether to skip waiting for the SQL warehouse to start after creation. Default is `false`. When set to `true`, Terraform will create the warehouse but won't wait for it to be in a running state before completing",
    )
    spot_instance_policy: str | None = Field(
        None,
        description="The spot policy to use for allocating instances to clusters: `COST_OPTIMIZED` or `RELIABILITY_OPTIMIZED`. This field is optional. Default is `COST_OPTIMIZED`",
    )
    warehouse_type: str | None = Field(
        None,
        description="SQL warehouse type. See for [AWS](https://docs.databricks.com/sql/admin/sql-endpoints.html#switch-the-sql-warehouse-type-pro-classic-or-serverless) or [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/admin/create-sql-warehouse#--upgrade-a-pro-or-classic-sql-warehouse-to-a-serverless-sql-warehouse). Set to `PRO` or `CLASSIC`. If the field `enable_serverless_compute` has the value `true` either explicitly or through the default logic (see that field above for details), the default is `PRO`, which is required for serverless SQL warehouses. Otherwise, the default is `CLASSIC`",
    )
    channel: SqlEndpointChannel | None = Field(None)
    tags: SqlEndpointTags | None = Field(
        None, description="Databricks tags all endpoint resources with these tags"
    )
    timeouts: SqlEndpointTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_endpoint"


__all__ = [
    "SqlEndpointChannel",
    "SqlEndpointTags",
    "SqlEndpointTagsCustomTags",
    "SqlEndpointTimeouts",
    "SqlEndpointBase",
]
