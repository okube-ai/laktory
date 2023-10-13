from typing import Literal
from pydantic import Field
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission


class ClusterAutoScale(BaseModel):
    min_workers: int
    max_workers: int


class ClusterInitScriptVolumes(BaseModel):
    destination: str = None


class ClusterInitScriptWorkspace(BaseModel):
    destination: str = None


class ClusterInitScript(BaseModel):
    volumes: ClusterInitScriptVolumes = None
    workspace: ClusterInitScriptWorkspace = None


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


class Cluster(BaseModel, Resources):
    name: str
    spark_version: str
    apply_policy_default_values: bool = None
    autoscale: ClusterAutoScale = None
    autotermination_minutes: int = None
    # aws_attributes
    # azure_attributes
    # cluster_log_conf
    # cluster_source
    custom_tags: dict[str, str] = None
    data_security_mode: Literal["NONE", "SINGLE_USER", "USER_ISOLATION"] = "USER_ISOLATION"
    # docker_image
    # driver_instance_pool_id
    # driver_node_type_id
    # enable_elastic_disk
    # enable_local_disk_encryption
    # gcp_attributes
    init_scripts: list[ClusterInitScript] = []
    # instance_pool_id
    libraries: list[ClusterLibrary] = []
    node_type_id: str = Field(...)
    num_workers: int = None
    # policy_id
    runtime_engine: Literal["Standard", "Photon"] = None
    single_user_name: str = None
    # spark_conf
    spark_env_vars: dict[str, str] = None
    # ssh_public_keys
    # workload_type

    # Deployment Options
    is_pinned: bool = True
    permissions: list[Permission] = []

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.cluster import PulumiCluster
        return PulumiCluster(name=name, cluster=self, opts=opts)
