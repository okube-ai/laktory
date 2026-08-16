# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_instance_pool
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class InstancePoolAwsAttributes(BaseModel):
    availability: str | None = Field(
        None,
        description="Availability type used for all nodes. Valid values are `SPOT_AZURE` and `ON_DEMAND_AZURE`",
    )
    instance_profile_arn: str | None = Field(
        None,
        description="Nodes belonging to the pool will only be placed on AWS instances with this instance profile. Please see databricks_instance_profile resource documentation for extended examples on adding a valid instance profile using Terraform",
    )
    spot_bid_price_percent: int | None = Field(
        None,
        description="(Integer) The max price for AWS spot instances, as a percentage of the corresponding instance type's on-demand price. For example, if this field is set to 50, and the instance pool needs a new i3.xlarge spot instance, then the max price is half of the price of on-demand i3.xlarge instances. Similarly, if this field is set to 200, the max price is twice the price of on-demand i3.xlarge instances. If not specified, the *default value is 100*. When spot instances are requested for this instance pool, only spot instances whose max price percentage matches this field are considered. *For safety, this field cannot be greater than 10000.*",
    )
    zone_id: str | None = Field(
        None,
        description="Identifier for the availability zone/datacenter in which the cluster resides. This string will be of a form like `us-central1-a`. The provided availability zone must be in the same region as the Databricks workspace",
    )


class InstancePoolAzureAttributes(BaseModel):
    availability: str | None = Field(
        None,
        description="Availability type used for all nodes. Valid values are `SPOT_AZURE` and `ON_DEMAND_AZURE`",
    )
    spot_bid_max_price: float | None = Field(
        None,
        description="The max bid price used for Azure spot instances. You can set this to greater than or equal to the current spot price. You can also set this to `-1`, which specifies that the instance cannot be evicted on the basis of price. The price for the instance will be the current price for spot instances or the price for a standard instance",
    )


class InstancePoolDiskSpecDiskType(BaseModel):
    azure_disk_volume_type: str | None = Field(None)
    ebs_volume_type: str | None = Field(None)


class InstancePoolDiskSpec(BaseModel):
    disk_count: int | None = Field(
        None,
        description="(Integer) The number of disks to attach to each instance. This feature is only enabled for supported node types. Users can choose up to the limit of the disks supported by the node type. For node types with no local disk, at least one disk needs to be specified",
    )
    disk_size: int | None = Field(
        None, description="(Integer) The size of each disk (in GiB) to attach"
    )
    disk_type: InstancePoolDiskSpecDiskType | None = Field(None)


class InstancePoolGcpAttributes(BaseModel):
    gcp_availability: str | None = Field(
        None,
        description="Availability type used for all nodes. Valid values are `PREEMPTIBLE_GCP`, `PREEMPTIBLE_WITH_FALLBACK_GCP` and `ON_DEMAND_GCP`, default: `ON_DEMAND_GCP`. * `local_ssd_count` (Optional, Int) Number of local SSD disks (each is 375GB in size) that will be attached to each node of the cluster",
    )
    local_ssd_count: int | None = Field(None)
    zone_id: str | None = Field(
        None,
        description="Identifier for the availability zone/datacenter in which the cluster resides. This string will be of a form like `us-central1-a`. The provided availability zone must be in the same region as the Databricks workspace",
    )


class InstancePoolInstancePoolFleetAttributesFleetOnDemandOption(BaseModel):
    allocation_strategy: str = Field(...)
    instance_pools_to_use_count: int | None = Field(None)


class InstancePoolInstancePoolFleetAttributesFleetSpotOption(BaseModel):
    allocation_strategy: str = Field(...)
    instance_pools_to_use_count: int | None = Field(None)


class InstancePoolInstancePoolFleetAttributesLaunchTemplateOverride(BaseModel):
    availability_zone: str = Field(...)
    instance_type: str = Field(...)


class InstancePoolInstancePoolFleetAttributes(BaseModel):
    fleet_on_demand_option: (
        InstancePoolInstancePoolFleetAttributesFleetOnDemandOption | None
    ) = Field(None)
    fleet_spot_option: InstancePoolInstancePoolFleetAttributesFleetSpotOption | None = (
        Field(None)
    )
    launch_template_override: (
        list[InstancePoolInstancePoolFleetAttributesLaunchTemplateOverride] | None
    ) = PluralField(None, plural="launch_template_overrides")


class InstancePoolNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] = Field(
        ...,
        description="list of alternative node types that will be used if main node type isn't available.  Follow the [documentation](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-types#fallback-instance-type-requirements) for requirements on selection of alternative node types",
    )


class InstancePoolPreloadedDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class InstancePoolPreloadedDockerImage(BaseModel):
    url: str = Field(..., description="URL for the Docker image")
    basic_auth: InstancePoolPreloadedDockerImageBasicAuth | None = Field(
        None,
        description="`basic_auth.username` and `basic_auth.password` for Docker repository. Docker registry credentials are encrypted when they are stored in Databricks internal storage and when they are passed to a registry upon fetching Docker images at cluster launch.  For better security, these credentials should be stored in the secret scope and referred using secret path syntax: `{{secrets/scope/key}}`, otherwise other users of the workspace may access them via UI/API",
    )


class InstancePoolBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_instance_pool`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    idle_instance_autotermination_minutes: int = Field(
        ...,
        description="(Integer) The number of minutes that idle instances in excess of the min_idle_instances are maintained by the pool before being terminated. If not specified, excess idle instances are terminated automatically after a default timeout period. If specified, the time must be between 0 and 10000 minutes. If you specify 0, excess idle instances are removed as soon as possible",
    )
    instance_pool_name: str = Field(
        ...,
        description="(String) The name of the instance pool. This is required for create and edit operations. It must be unique, non-empty, and less than 100 characters",
    )
    custom_tags: dict[str, str] | None = Field(
        None,
        description="(Map) Additional tags for instance pool resources. Databricks tags all pool resources (e.g. AWS & Azure instances and Disk volumes). The tags of the instance pool will propagate to the clusters using the pool (see the [official documentation](https://docs.databricks.com/administration-guide/account-settings/usage-detail-tags-aws.html#tag-propagation)). Attempting to set the same tags in both cluster and instance pool will raise an error. *Databricks allows at most 43 custom tags.*",
    )
    enable_elastic_disk: bool | None = Field(
        None,
        description="(Bool) Autoscaling Local Storage: when enabled, the instances in the pool dynamically acquire additional disk space when they are running low on disk space",
    )
    instance_pool_id: str | None = Field(None)
    max_capacity: int | None = Field(
        None,
        description="(Integer) The maximum number of instances the pool can contain, including both idle instances and ones in use by clusters. Once the maximum capacity is reached, you cannot create new clusters from the pool and existing clusters cannot autoscale up until some instances are made idle in the pool via cluster termination or down-scaling. There is no default limit, but as a [best practice](https://docs.databricks.com/clusters/instance-pools/pool-best-practices.html#configure-pools-to-control-cost), this should be set based on anticipated usage",
    )
    min_idle_instances: int | None = Field(
        None,
        description="(Integer) The minimum number of idle instances maintained by the pool. This is in addition to any instances in use by active clusters",
    )
    node_type_id: str | None = Field(
        None,
        description="(String) The node type for the instances in the pool. All clusters attached to the pool inherit this node type and the pool's idle instances are allocated based on this type. You can retrieve a list of available node types by using the [List Node Types API](https://docs.databricks.com/dev-tools/api/latest/clusters.html#clusterclusterservicelistnodetypes) call",
    )
    preloaded_spark_versions: list[str] | None = Field(
        None,
        description="(List) A list with at most one runtime version the pool installs on each instance. Pool clusters that use a preloaded runtime version start faster as they do not have to wait for the image to download. You can retrieve them via databricks_spark_version data source or via  [Runtime Versions API](https://docs.databricks.com/dev-tools/api/latest/clusters.html#clusterclusterservicelistsparkversions) call",
    )
    aws_attributes: InstancePoolAwsAttributes | None = Field(None)
    azure_attributes: InstancePoolAzureAttributes | None = Field(None)
    disk_spec: InstancePoolDiskSpec | None = Field(None)
    gcp_attributes: InstancePoolGcpAttributes | None = Field(None)
    instance_pool_fleet_attributes: InstancePoolInstancePoolFleetAttributes | None = (
        Field(None)
    )
    node_type_flexibility: InstancePoolNodeTypeFlexibility | None = Field(
        None,
        description="a block describing the alternative driver node types if `node_type_id` isn't available",
    )
    preloaded_docker_image: list[InstancePoolPreloadedDockerImage] | None = PluralField(
        None, plural="preloaded_docker_images"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_instance_pool"


__all__ = [
    "InstancePoolAwsAttributes",
    "InstancePoolAzureAttributes",
    "InstancePoolBase",
    "InstancePoolDiskSpec",
    "InstancePoolDiskSpecDiskType",
    "InstancePoolGcpAttributes",
    "InstancePoolInstancePoolFleetAttributes",
    "InstancePoolInstancePoolFleetAttributesFleetOnDemandOption",
    "InstancePoolInstancePoolFleetAttributesFleetSpotOption",
    "InstancePoolInstancePoolFleetAttributesLaunchTemplateOverride",
    "InstancePoolNodeTypeFlexibility",
    "InstancePoolPreloadedDockerImage",
    "InstancePoolPreloadedDockerImageBasicAuth",
]
