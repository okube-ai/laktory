import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.cluster import Cluster

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiCluster(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            cluster: Cluster = None,
            opts=None,
    ):
        if name is None:
            name = f"cluster-{cluster.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        autoscale = None
        if cluster.autoscale:
            autoscale = databricks.ClusterAutoscaleArgs(
                min_workers=cluster.autoscale.min_workers,
                max_workers=cluster.autoscale.max_workers,
            )

        init_scripts = []
        for i in cluster.init_scripts:
            if i.volumes:
                init_scripts += [
                    databricks.ClusterInitScriptArgs(
                        volumes=databricks.ClusterInitScriptVolumesArgs(destination=i.volumes.destination)
                    )
                ]

            if i.workspace:
                init_scripts += [
                    databricks.ClusterInitScriptArgs(
                        workspace=databricks.ClusterInitScriptWorkspaceArgs(destination=i.volumes.destination)
                    )
                ]

        libraries = []
        for l in cluster.libraries:
            if l.cran:
                libraries += [databricks.ClusterLibraryArgs(
                    cran=databricks.ClusterLibraryCranArgs(
                        package=l.cran.package,
                        repo=l.cran.repo,
                    )
                )]
            if l.egg:
                libraries += [databricks.ClusterLibraryArgs(
                    egg=l.egg
                )]
            if l.jar:
                libraries += [databricks.ClusterLibraryArgs(
                    jar=l.jar
                )]
            if l.maven:
                libraries += [databricks.ClusterLibraryArgs(
                    maven=databricks.ClusterLibraryMavenArgs(
                        coordinates=l.maven.coordinates,
                        exclusions=l.maven.exclusions,
                        repo=l.maven.repo,
                    )
                )]
            if l.pypi:
                libraries += [databricks.ClusterLibraryArgs(
                    pypi=databricks.ClusterLibraryPypiArgs(
                        package=l.pypi.package,
                        repo=l.pypi.repo,
                    )
                )]
            if l.whl:
                libraries += [databricks.ClusterLibraryArgs(
                    whl=l.whl
                )]

        self.cluster = databricks.Cluster(
                f"cluster-{cluster.name}",
                apply_policy_default_values=cluster.apply_policy_default_values,
                autoscale=autoscale,
                autotermination_minutes=cluster.autotermination_minutes,
                cluster_name=cluster.name,
                custom_tags=cluster.custom_tags,
                data_security_mode=cluster.data_security_mode,
                init_scripts=init_scripts,
                libraries=libraries,
                node_type_id=cluster.node_type_id,
                num_workers=cluster.num_workers,
                runtime_engine=cluster.runtime_engine,
                single_user_name=cluster.single_user_name,
                spark_env_vars=cluster.spark_env_vars,
                spark_version=cluster.spark_version,
                is_pinned=cluster.is_pinned,
                opts=opts,
            )

        access_controls = []
        for permission in cluster.permissions:
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
                f"permissions-cluster-{cluster.name}",
                access_controls=access_controls,
                cluster_id=self.cluster.id,
                opts=opts,
            )
