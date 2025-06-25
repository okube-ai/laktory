from typing import Literal
from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ClusterAutoScale(BaseModel):
    min_workers: int = Field(..., description="Minimum number of worker nodes")
    max_workers: int = Field(..., description="Maximum number of worker nodes")


class ClusterInitScriptVolumes(BaseModel):
    destination: str = Field(None, description="Volume filepath")


class ClusterInitScriptWorkspace(BaseModel):
    destination: str = Field(None, description="Workspace filepath")


class ClusterInitScript(BaseModel):
    volumes: ClusterInitScriptVolumes = Field(
        None, description="Volumes file specification"
    )
    workspace: ClusterInitScriptWorkspace = Field(
        None, description="Workspace file specifications"
    )


class ClusterLibraryCran(BaseModel):
    package: str = Field(None, description="")
    repo: str = Field(None, description="")


class ClusterLibraryMaven(BaseModel):
    coordinates: str = Field(None, description="")
    exclusions: list[str] = Field(None, description="")
    repo: str = Field(None, description="")


class ClusterLibraryPypi(BaseModel):
    package: str = Field(None, description="Package name")
    repo: str = Field(None, description="Packages repository")


class ClusterLibrary(BaseModel):
    cran: ClusterLibraryCran = Field(None, description="Cran library specifications")
    egg: str = Field(None, description="Egg filepath")
    jar: str = Field(None, description="Jar filepath")
    maven: ClusterLibraryMaven = Field(None, description="Maven specifications")
    pypi: ClusterLibraryPypi = Field(None, description="Pypi library specifications")
    whl: str = Field(None, description="Wheel filepath")


class ClusterLookup(ResourceLookup):
    cluster_id: str = Field(
        serialization_alias="id", description="The id of the cluster"
    )


class Cluster(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks cluster

    Examples
    --------
    ```py
    from laktory import models

    cluster = models.resources.databricks.Cluster(
        name="default",
        spark_version="16.3.x-scala2.12",
        data_security_mode="USER_ISOLATION",
        node_type_id="Standard_DS3_v2",
        autoscale={
            "min_workers": 1,
            "max_workers": 4,
        },
        num_workers=0,
        autotermination_minutes=30,
        libraries=[{"pypi": {"package": "laktory==0.0.23"}}],
        access_controls=[
            {
                "group_name": "role-engineers",
                "permission_level": "CAN_RESTART",
            }
        ],
        is_pinned=True,
    )
    ```

    References
    ----------

    * [Databricks Cluster](https://docs.databricks.com/en/compute/configure.html#autoscaling-local-storage-1)
    * [Pulumi Databricks Cluster](https://www.pulumi.com/registry/packages/databricks/api-docs/cluster/)

    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )
    apply_policy_default_values: bool = Field(
        None,
        description="Whether to use policy default values for missing cluster attributes.",
    )
    autoscale: ClusterAutoScale = Field(None, description="Autoscale specifications")
    autotermination_minutes: int = Field(
        None,
        description="Automatically terminate the cluster after being inactive for this time in minutes.",
    )
    # aws_attributes
    # azure_attributes
    cluster_id: str = Field(
        None, description="Cluster ID. Used when assigning a cluster to a job task."
    )
    # cluster_log_conf
    # cluster_source
    # cluster_mount_infos
    custom_tags: dict[str, str] = Field(
        None,
        description="""
    Additional tags for cluster resources. Databricks will tag all cluster resources (e.g., AWS EC2 instances and EBS
    volumes) with these tags in addition to default_tags. If a custom cluster tag has the same name as a default
    cluster tag, the custom tag is prefixed with an x_ when it is propagated.
    """,
    )
    data_security_mode: Literal["NONE", "SINGLE_USER", "USER_ISOLATION"] = Field(
        "USER_ISOLATION",
        description="""
        Select the security features of the cluster. Unity Catalog requires SINGLE_USER or USER_ISOLATION mode. If 
        omitted, no security features are enabled. In the Databricks UI, this has been recently been renamed Access
        Mode and USER_ISOLATION has been renamed Shared, but use these terms here.
        """,
    )
    # docker_image
    driver_instance_pool_id: str = Field(
        None,
        description="""
    Similar to instance_pool_id, but for driver node. If omitted, and instance_pool_id is specified, then the driver 
    will be allocated from that pool.
    """,
    )
    driver_node_type_id: str = Field(
        None,
        description="""
    The node type of the Spark driver. This field is optional; if unset, API will set the driver node type to the same
    value as node_type_id defined above.
    """,
    )
    enable_elastic_disk: bool = Field(
        None,
        description="""
    If you don’t want to allocate a fixed number of EBS volumes at cluster creation time, use autoscaling local 
    storage. With autoscaling local storage, Databricks monitors the amount of free disk space available on your
    cluster’s Spark workers. If a worker begins to run too low on disk, Databricks automatically attaches a new EBS 
    volume to the worker before it runs out of disk space. EBS volumes are attached up to a limit of 5 TB of total 
    disk space per instance (including the instance’s local storage). To scale down EBS usage, make sure you have
    autotermination_minutes and autoscale attributes set.
    """,
    )
    enable_local_disk_encryption: bool = Field(
        None,
        description="""
    Some instance types you use to run clusters may have locally attached disks. Databricks may store shuffle data or
    temporary data on these locally attached disks. To ensure that all data at rest is encrypted for all storage types,
    including shuffle data stored temporarily on your cluster’s local disks, you can enable local disk encryption. When
    local disk encryption is enabled, Databricks generates an encryption key locally unique to each cluster node and 
    uses it to encrypt all data stored on local disks. The scope of the key is local to each cluster node and is
    destroyed along with the cluster node itself. During its lifetime, the key resides in memory for encryption and 
    decryption and is stored encrypted on the disk. Your workloads may run more slowly because of the performance 
    impact of reading and writing encrypted data to and from local volumes. This feature is not available for all 
    Azure Databricks subscriptions. Contact your Microsoft or Databricks account representative to request access.
    """,
    )
    # gcp_attributes
    idempotency_token: str = Field(
        None,
        description="""
    An optional token to guarantee the idempotency of cluster creation requests. If an active cluster with the provided
    token already exists, the request will not create a new cluster, but it will return the existing running cluster's
    ID instead. If you specify the idempotency token, upon failure, you can retry until the request succeeds.
    Databricks platform guarantees to launch exactly one cluster with that idempotency token. This token should have 
    at most 64 characters.
    """,
    )
    init_scripts: list[ClusterInitScript] = Field(
        [], description="List of init scripts specifications"
    )
    instance_pool_id: str = Field(
        None,
        description="""
    To reduce cluster start time, you can attach a cluster to a predefined pool of idle instances. When attached to a 
    pool, a cluster allocates its driver and worker nodes from the pool. If the pool does not have sufficient idle 
    resources to accommodate the cluster’s request, it expands by allocating new instances from the instance provider. 
    When an attached cluster changes its state to TERMINATED, the instances it used are returned to the pool and 
    reused by a different cluster.
    """,
    )
    is_pinned: bool = Field(
        True,
        description="""
    boolean value specifying if the cluster is pinned (not pinned by default). You must be a Databricks administrator 
    to use this. The pinned clusters' maximum number is limited to 100, so apply may fail if you have more than that 
    (this number may change over time, so check Databricks documentation for actual number).
    """,
    )
    libraries: list[ClusterLibrary] = Field(
        [], description="List of libraries specifications"
    )
    lookup_existing: ClusterLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    name: str = Field(
        None,
        description="""
    Cluster name, which doesn’t have to be unique. If not specified at creation, the cluster name will be an empty string.
    """,
    )
    node_type_id: str = Field(
        ...,
        description="Any supported databricks.getNodeType id. If instance_pool_id is specified, this field is not needed.",
    )
    no_wait: bool = Field(
        None,
        description="""
    If true, the provider will not wait for the cluster to reach RUNNING state when creating the cluster, allowing 
    cluster creation and library installation to continue asynchronously. Defaults to false (the provider will wait 
    for cluster creation and library installation to succeed).
    """,
    )
    num_workers: int = Field(
        None,
        description="""
    Number of worker nodes that this cluster should have. A cluster has one Spark driver and num_workers executors
    for a total of num_workers + 1 Spark nodes.
    """,
    )
    policy_id: str = Field(None, description="")
    runtime_engine: Literal["STANDARD", "PHOTON"] = Field(
        None,
        description="""
    The type of runtime engine to use. If not specified, the runtime engine type is inferred based on the 
    spark_version value
    """,
    )
    single_user_name: str = Field(
        None,
        description="""
    The optional user name of the user to assign to an interactive cluster. This field is required when using 
    data_security_mode set to SINGLE_USER or AAD Passthrough for Azure Data Lake Storage (ADLS) with a single-user
    cluster (i.e., not high-concurrency clusters).
    """,
    )
    spark_conf: dict[str, str] = Field(
        {},
        description="""
    Map with key-value pairs to fine-tune Spark clusters, where you can provide custom Spark configuration
    properties in a cluster configuration.
    """,
    )
    spark_env_vars: dict[str, str] = Field(
        {},
        description="""
    Map with environment variable key-value pairs to fine-tune Spark clusters. Key-value pairs of the form (X,Y)
    are exported (i.e., X='Y') while launching the driver and workers.
    """,
    )
    spark_version: str = Field(
        ...,
        description="""
    Runtime version of the cluster. Any supported databricks.getSparkVersion id. We advise using Cluster Policies
    to restrict the list of versions for simplicity while maintaining enough control.
    """,
    )
    ssh_public_keys: list[str] = Field(
        [],
        description="""
    SSH public key contents that will be added to each Spark node in this cluster. The corresponding private keys
    can be used to login with the user name ubuntu on port 2200. You can specify up to 10 keys.
    """,
    )
    # workload_type:

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    cluster_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Cluster"

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"name": "cluster_name"}

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "init_scripts": "init_scripts",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_cluster"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
