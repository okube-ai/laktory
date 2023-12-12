from typing import Literal
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission


class WarehouseCustomTag(BaseModel):
    """
    Warehouse Custom Tag specifications

    Attributes
    ----------
    key:
        Tag key
    value:
        Tag value
    """

    key: str
    value: str


class WarehouseTags(BaseModel):
    """
    Warehouse Tags specifications

    Attributes
    ----------
    custom_tags:
        Tags specifications
    """

    custom_tags: list[WarehouseCustomTag] = []


class Warehouse(BaseModel, BaseResource):
    """
    Databricks Warehouse

    Attributes
    ----------
    auto_stop_mins:
        Time in minutes until an idle SQL warehouse terminates all clusters and stops.
    channel_name:
        Channel specifications
    cluster_size:
        The size of the clusters allocated to the endpoint
    enable_photon:
        If `True`, photon is enabled
    enable_serverless_compute:
        If `True`, warehouse is serverless.
        See details [here](https://www.pulumi.com/registry/packages/databricks/api-docs/sqlendpoint/)
    instance_profile_arn:
        TODO
    jdbc_url:
        JDBC connection string.
    max_num_clusters:
        Maximum number of clusters available when a SQL warehouse is running.
    min_num_clusters:
        Minimum number of clusters available when a SQL warehouse is running.
    name:
        Warehouse name
    num_clusters:
        Fixed number of clusters when autoscaling is not enabled.
    permissions:
        Warehouse permissions
    spot_instance_policy:
        The spot policy to use for allocating instances to clusters.
    tags:
        Databricks tags all endpoint resources with these tags.
    warehouse_type:
        SQL warehouse type.

    Examples
    --------
    ```py
    from laktory import models

    warehouse = models.Warehouse(
        name="default",
        cluster_size="2X-Small",
        auto_stop_mins=30,
        channel_name="CHANNEL_NAME_PREVIEW",
        enable_photon=True,
        enable_serverless_compute=True,
        permissions=[{"group_name": "account users", "permission_level": "CAN_USE"}],
    )
    ```
    """

    cluster_size: Literal[
        "2X-Small",
        "X-Small",
        "Small",
        "Medium",
        "Large",
        "X-Large",
        "2X-Large",
        "3X-Large",
        "4X-Large",
    ]
    auto_stop_mins: int = None
    channel_name: Literal["CHANNEL_NAME_CURRENT", "CHANNEL_NAME_PREVIEW"] = None
    # data_source_id
    enable_photon: bool = None
    enable_serverless_compute: bool = None
    instance_profile_arn: str = None
    jdbc_url: str = None
    max_num_clusters: int = None
    min_num_clusters: int = None
    name: str = None
    num_clusters: int = None
    # odbc_params
    permissions: list[Permission] = []
    spot_instance_policy: Literal["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"] = None
    # state
    tags: WarehouseTags = None
    warehouse_type: Literal["CLASSIC", "PRO"] = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions"]

    def model_pulumi_dump(self, *args, **kwargs):
        d = super().model_pulumi_dump(*args, **kwargs)
        d["channel"] = {"name": d.pop("channel_name")}
        return d

    def deploy_with_pulumi(self, name=None, opts=None):
        """
        Deploy warehouse using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiWarehouse:
            Pulumi warehouse resource
        """
        from laktory.resourcesengines.pulumi.warehouse import PulumiWarehouse

        return PulumiWarehouse(name=name, warehouse=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    warehouse = models.Warehouse(
        name="default",
        cluster_size="2X-Small",
        auto_stop_mins=30,
        channel_name="CHANNEL_NAME_PREVIEW",
        enable_photon=True,
        enable_serverless_compute=True,
        permissions=[{"group_name": "account users", "permission_level": "CAN_USE"}],
    )
