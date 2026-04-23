# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_cluster
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class ClusterAutoscale(BaseModel):
    max_workers: float | None = Field(None)
    min_workers: float | None = Field(None)


class ClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: float | None = Field(None)
    ebs_volume_iops: float | None = Field(None)
    ebs_volume_size: float | None = Field(None)
    ebs_volume_throughput: float | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: float | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: float | None = Field(None)
    zone_id: str | None = Field(None)


class ClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class ClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: float | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: ClusterAzureAttributesLogAnalyticsInfo | None = Field(None)


class ClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class ClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class ClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class ClusterClusterLogConf(BaseModel):
    dbfs: ClusterClusterLogConfDbfs | None = Field(None)
    s3: ClusterClusterLogConfS3 | None = Field(None)
    volumes: ClusterClusterLogConfVolumes | None = Field(None)


class ClusterClusterMountInfoNetworkFilesystemInfo(BaseModel):
    mount_options: str | None = Field(None)
    server_address: str = Field(...)


class ClusterClusterMountInfo(BaseModel):
    local_mount_dir_path: str = Field(...)
    remote_mount_dir_path: str | None = Field(None)
    network_filesystem_info: ClusterClusterMountInfoNetworkFilesystemInfo | None = (
        Field(None)
    )


class ClusterDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class ClusterDockerImage(BaseModel):
    url: str = Field(...)
    basic_auth: ClusterDockerImageBasicAuth | None = Field(None)


class ClusterDriverNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class ClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    boot_disk_size: float | None = Field(None)
    first_on_demand: float | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: float | None = Field(None)
    use_preemptible_executors: bool | None = Field(None)
    zone_id: str | None = Field(None)


class ClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class ClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class ClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class ClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class ClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class ClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class ClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class ClusterInitScripts(BaseModel):
    abfss: ClusterInitScriptsAbfss | None = Field(None)
    dbfs: ClusterInitScriptsDbfs | None = Field(None)
    file: ClusterInitScriptsFile | None = Field(None)
    gcs: ClusterInitScriptsGcs | None = Field(None)
    s3: ClusterInitScriptsS3 | None = Field(None)
    volumes: ClusterInitScriptsVolumes | None = Field(None)
    workspace: ClusterInitScriptsWorkspace | None = Field(None)


class ClusterLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class ClusterLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class ClusterLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class ClusterLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: ClusterLibraryCran | None = Field(None)
    maven: ClusterLibraryMaven | None = Field(None)
    pypi: ClusterLibraryPypi | None = Field(None)


class ClusterTimeouts(BaseModel):
    create: str | None = Field(None)
    delete: str | None = Field(None)
    update_: str | None = Field(
        None,
        serialization_alias="update",
        validation_alias=AliasChoices("update", "update_"),
    )


class ClusterWorkerNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class ClusterWorkloadTypeClients(BaseModel):
    jobs: bool | None = Field(None)
    notebooks: bool | None = Field(None)


class ClusterWorkloadType(BaseModel):
    clients: ClusterWorkloadTypeClients | None = Field(None)


class ClusterBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_cluster`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    spark_version: str = Field(
        ...,
        description="The Spark version of the cluster, e.g. `3.3.x-scala2.11`. A list of available Spark versions can be retrieved by using the :method:clusters/sparkVersions API call.",
    )
    apply_policy_default_values: bool | None = Field(
        None,
        description="When set to true, fixed and default values from the policy will be used for fields that are omitted. When set to false, only fixed values from the policy will be applied.",
    )
    autotermination_minutes: float | None = Field(
        None,
        description="Automatically terminates the cluster after it is inactive for this time in minutes. If not set, this cluster will not be automatically terminated. If specified, the threshold must be between 10 and 10000 minutes. Users can also set this value to 0 to explicitly disable automatic termination.",
    )
    cluster_name: str | None = Field(
        None,
        description="Cluster name requested by the user. This doesn't have to be unique. If not specified at creation, the cluster name will be an empty string. For job clusters, the cluster name is automatically set based on the job and job run IDs.",
    )
    custom_tags: dict[str, str] | None = Field(
        None,
        description="Additional tags for cluster resources. Databricks will tag all cluster resources (e.g., AWS instances and EBS volumes) with these tags in addition to `default_tags`. Notes:",
    )
    data_security_mode: str | None = Field(None)
    driver_instance_pool_id: str | None = Field(
        None,
        description="The optional ID of the instance pool for the driver of the cluster belongs. The pool cluster uses the instance pool with id (instance_pool_id) if the driver pool is not assigned.",
    )
    driver_node_type_id: str | None = Field(
        None,
        description="The node type of the Spark driver. Note that this field is optional; if unset, the driver node type will be set as the same value as `node_type_id` defined above.",
    )
    enable_elastic_disk: bool | None = Field(
        None,
        description="Autoscaling Local Storage: when enabled, this cluster will dynamically acquire additional disk space when its Spark workers are running low on disk space.",
    )
    enable_local_disk_encryption: bool | None = Field(
        None, description="Whether to enable LUKS on cluster VMs' local disks"
    )
    idempotency_token: str | None = Field(None)
    instance_pool_id: str | None = Field(
        None,
        description="The optional ID of the instance pool to which the cluster belongs.",
    )
    is_pinned: bool | None = Field(None)
    is_single_node: bool | None = Field(
        None, description="This field can only be used when `kind = CLASSIC_PREVIEW`."
    )
    kind: str | None = Field(None)
    no_wait: bool | None = Field(None)
    node_type_id: str | None = Field(
        None,
        description="This field encodes, through a single value, the resources available to each of the Spark nodes in this cluster. For example, the Spark nodes can be provisioned and optimized for memory or compute intensive workloads. A list of available node types can be retrieved by using the :method:clusters/listNodeTypes API call.",
    )
    num_workers: float | None = Field(
        None,
        description="Number of worker nodes that this cluster should have. A cluster has one Spark Driver and `num_workers` Executors for a total of `num_workers` + 1 Spark nodes.",
    )
    policy_id: str | None = Field(
        None,
        description="The ID of the cluster policy used to create the cluster if applicable.",
    )
    remote_disk_throughput: float | None = Field(
        None,
        description="If set, what the configurable throughput (in Mb/s) for the remote disk is. Currently only supported for GCP HYPERDISK_BALANCED disks.",
    )
    runtime_engine: str | None = Field(
        None,
        description="Determines the cluster's runtime engine, either standard or Photon.",
    )
    single_user_name: str | None = Field(
        None, description="Single user name if data_security_mode is `SINGLE_USER`"
    )
    spark_conf: dict[str, str] | None = Field(
        None,
        description="An object containing a set of optional, user-specified Spark configuration key-value pairs. Users can also pass in a string of extra JVM options to the driver and the executors via `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` respectively.",
    )
    spark_env_vars: dict[str, str] | None = Field(
        None,
        description="An object containing a set of optional, user-specified environment variable key-value pairs. Please note that key-value pair of the form (X,Y) will be exported as is (i.e., `export X='Y'`) while launching the driver and workers.",
    )
    ssh_public_keys: list[str] | None = Field(
        None,
        description="SSH public key contents that will be added to each Spark node in this cluster. The corresponding private keys can be used to login with the user name `ubuntu` on port `2200`. Up to 10 keys can be specified.",
    )
    total_initial_remote_disk_size: float | None = Field(
        None,
        description="If set, what the total initial volume size (in GB) of the remote disks should be. Currently only supported for GCP HYPERDISK_BALANCED disks.",
    )
    use_ml_runtime: bool | None = Field(
        None, description="This field can only be used when `kind = CLASSIC_PREVIEW`."
    )
    autoscale: ClusterAutoscale | None = Field(
        None,
        description="Parameters needed in order to automatically scale clusters up and down based on load. Note: autoscaling works best with DB runtime versions 3.0 or later.",
    )
    aws_attributes: ClusterAwsAttributes | None = Field(
        None,
        description="Attributes related to clusters running on Amazon Web Services. If not specified at cluster creation, a set of default values will be used.",
    )
    azure_attributes: ClusterAzureAttributes | None = Field(
        None,
        description="Attributes related to clusters running on Microsoft Azure. If not specified at cluster creation, a set of default values will be used.",
    )
    cluster_log_conf: ClusterClusterLogConf | None = Field(
        None,
        description="The configuration for delivering spark logs to a long-term storage destination. Three kinds of destinations (DBFS, S3 and Unity Catalog volumes) are supported. Only one destination can be specified for one cluster. If the conf is given, the logs will be delivered to the destination every `5 mins`. The destination of driver logs is `$destination/$clusterId/driver`, while the destination of executor logs is `$destination/$clusterId/executor`.",
    )
    cluster_mount_info: list[ClusterClusterMountInfo] | None = PluralField(
        None, plural="cluster_mount_infos"
    )
    docker_image: ClusterDockerImage | None = Field(
        None, description="Custom docker image BYOC"
    )
    driver_node_type_flexibility: ClusterDriverNodeTypeFlexibility | None = Field(
        None, description="Flexible node type configuration for the driver node."
    )
    gcp_attributes: ClusterGcpAttributes | None = Field(
        None,
        description="Attributes related to clusters running on Google Cloud Platform. If not specified at cluster creation, a set of default values will be used.",
    )
    init_scripts: list[ClusterInitScripts] | None = Field(
        None,
        description="The configuration for storing init scripts. Any number of destinations can be specified. The scripts are executed sequentially in the order provided. If `cluster_log_conf` is specified, init script logs are sent to `<destination>/<cluster-ID>/init_scripts`.",
    )
    library: list[ClusterLibrary] | None = PluralField(None, plural="libraries")
    timeouts: ClusterTimeouts | None = Field(None)
    worker_node_type_flexibility: ClusterWorkerNodeTypeFlexibility | None = Field(
        None, description="Flexible node type configuration for worker nodes."
    )
    workload_type: ClusterWorkloadType | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_cluster"


__all__ = [
    "ClusterAutoscale",
    "ClusterAwsAttributes",
    "ClusterAzureAttributes",
    "ClusterAzureAttributesLogAnalyticsInfo",
    "ClusterClusterLogConf",
    "ClusterClusterLogConfDbfs",
    "ClusterClusterLogConfS3",
    "ClusterClusterLogConfVolumes",
    "ClusterClusterMountInfo",
    "ClusterClusterMountInfoNetworkFilesystemInfo",
    "ClusterDockerImage",
    "ClusterDockerImageBasicAuth",
    "ClusterDriverNodeTypeFlexibility",
    "ClusterGcpAttributes",
    "ClusterInitScripts",
    "ClusterInitScriptsAbfss",
    "ClusterInitScriptsDbfs",
    "ClusterInitScriptsFile",
    "ClusterInitScriptsGcs",
    "ClusterInitScriptsS3",
    "ClusterInitScriptsVolumes",
    "ClusterInitScriptsWorkspace",
    "ClusterLibrary",
    "ClusterLibraryCran",
    "ClusterLibraryMaven",
    "ClusterLibraryPypi",
    "ClusterTimeouts",
    "ClusterWorkerNodeTypeFlexibility",
    "ClusterWorkloadType",
    "ClusterWorkloadTypeClients",
    "ClusterBase",
]
