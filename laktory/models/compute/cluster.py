from typing import Literal
from pydantic import Field
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission


class ClusterAutoScale(BaseModel):
    min_workers: int
    max_workers: int

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.ClusterAutoscaleArgs(**self.model_dump())


class ClusterInitScriptVolumes(BaseModel):
    destination: str = None


class ClusterInitScriptWorkspace(BaseModel):
    destination: str = None


class ClusterInitScript(BaseModel):
    volumes: ClusterInitScriptVolumes = None
    workspace: ClusterInitScriptWorkspace = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.ClusterInitScriptArgs(**self.model_dump())


class ClusterLibraryCran(BaseModel):
    package: str = None
    repo: str = None


class ClusterLibraryMaven(BaseModel):
    coordinates: str = None
    exclusions: list[str] = None
    repo: str = None


class ClusterLibraryPypi(BaseModel):
    package: str = None
    repo: str = None


class ClusterLibrary(BaseModel):
    cran: ClusterLibraryCran = None
    egg: str = None
    jar: str = None
    maven: ClusterLibraryMaven = None
    pypi: ClusterLibraryPypi = None
    whl: str = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.ClusterLibraryArgs(**self.model_dump())


class Cluster(BaseModel, Resources):
    apply_policy_default_values: bool = None
    autoscale: ClusterAutoScale = None
    autotermination_minutes: int = None
    # aws_attributes
    # azure_attributes
    cluster_id: str = None
    # cluster_log_conf
    # cluster_source
    # cluster_mount_infos
    custom_tags: dict[str, str] = None
    data_security_mode: Literal["NONE", "SINGLE_USER", "USER_ISOLATION"] = "USER_ISOLATION"
    # docker_image
    driver_instance_pool_id: str = None
    driver_node_type_id: str = None
    enable_elastic_disk: bool = None
    enable_local_disk_encryption: bool = True
    # gcp_attributes
    idempotency_token: str = None
    init_scripts: list[ClusterInitScript] = []
    instance_pool_id: str = None
    is_pinned: bool = True
    libraries: list[ClusterLibrary] = []
    name: str = None
    node_type_id: str = Field(...)
    num_workers: int = None
    permissions: list[Permission] = []
    policy_id: str = None
    runtime_engine: Literal["Standard", "Photon"] = None
    single_user_name: str = None
    spark_conf: dict[str, str] = {}
    spark_env_vars: dict[str, str] = {}
    spark_version: str
    ssh_public_keys: list[str] = []
    # workload_type:

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.cluster import PulumiCluster
        return PulumiCluster(name=name, cluster=self, opts=opts)
