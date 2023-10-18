from typing import Literal
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission


class WarehouseCustomTag(BaseModel):
    key: str
    value: str


class WarehouseTags(BaseModel):
    custom_tags: list[WarehouseCustomTag] = []

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.SqlEndpointTagsArgs(**self.model_dump())


class Warehouse(BaseModel, Resources):
    cluster_size: Literal["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
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
    spot_instance_policy: Literal["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"] = None
    # state
    tags: WarehouseTags = None
    warehouse_type: Literal["CLASSIC", "PRO"] = None

    # Deployment Options
    permissions: list[Permission] = []

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.warehouse import PulumiWarehouse
        return PulumiWarehouse(name=name, warehouse=self, opts=opts)
