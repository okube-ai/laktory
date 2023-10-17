from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.pipeline import Pipeline

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiPipeline(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            pipeline: Pipeline = None,
            opts=None,
    ):
        if name is None:
            name = f"pipline-{pipeline.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        clusters = []
        for c in pipeline.clusters:
            clusters += [
                databricks.PipelineClusterArgs(
                    f"cluster-{c.name}",
                    apply_policy_default_values=c.apply_policy_default_values,
                    autoscale=getattr(c.autoscale, "pulumi_args", None),
                    autotermination_minutes=c.autotermination_minutes,
                    cluster_id=c.cluster_id,
                    cluster_name=c.name,
                    custom_tags=c.custom_tags,
                    data_security_mode=c.data_security_mode,
                    driver_instance_pool_id=c.driver_instance_pool_id,
                    driver_node_type_id=c.driver_node_type_id,
                    enable_elastic_disk=c.enable_elastic_disk,
                    enable_local_disk_encryption=c.enable_local_disk_encryption,
                    idempotency_token=c.idempotency_token,
                    init_scripts=[i.pulumi_args for i in c.init_scripts],
                    instance_pool_id=c.instance_pool_id,
                    is_pinned=c.is_pinned,
                    libraries=[l.pulumi_args for l in c.libraries],
                    node_type_id=c.node_type_id,
                    num_workers=c.num_workers,
                    policy_id=c.policy_id,
                    runtime_engine=c.runtime_engine,
                    single_user_name=c.single_user_name,
                    spark_conf=c.spark_conf,
                    spark_env_vars=c.spark_env_vars,
                    spark_version=c.spark_version,
                    ssh_public_keys=c.ssh_public_keys,
                    opts=opts,
                )
            ]

        self.pipeline = databricks.Pipeline(
                f"pipline-{pipeline.name}",
                allow_duplicate_names=pipeline.allow_duplicate_names,
                catalog=pipeline.catalog,
                channel=pipeline.channel,
                clusters=[c.pulumi_args for c in pipeline.clusters],
                configuration=pipeline.configuration,
                continuous=pipeline.continuous,
                development=pipeline.development,
                edition=pipeline.edition,
                # filters=pipeline.filters,
                libraries=[l.pulumi_args for l in pipeline.libraries],
                name=pipeline.name,
                notifications=[n.pulumi_args for n in pipeline.notifications],
                photon=pipeline.photon,
                serverless=pipeline.serverless,
                storage=pipeline.storage,
                target=pipeline.target,
                opts=opts,
            )

        access_controls = []
        for permission in pipeline.permissions:
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
                f"permissions-pipeline-{pipeline.key}",
                access_controls=access_controls,
                workspace_file_path=self.pipeline.path,
                opts=opts,
            )
