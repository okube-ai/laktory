from typing import Union
import hashlib
import json
import os
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
                    apply_policy_default_values=c.apply_policy_default_values,
                    autoscale=getattr(c.autoscale, "pulumi_args", None),
                    custom_tags=c.custom_tags,
                    driver_instance_pool_id=c.driver_instance_pool_id,
                    driver_node_type_id=c.driver_node_type_id,
                    enable_local_disk_encryption=c.enable_local_disk_encryption,
                    init_scripts=[i.pulumi_args for i in c.init_scripts],
                    instance_pool_id=c.instance_pool_id,
                    label=c.name,
                    node_type_id=c.node_type_id,
                    num_workers=c.num_workers,
                    policy_id=c.policy_id,
                    spark_conf=c.spark_conf,
                    spark_env_vars=c.spark_env_vars,
                    ssh_public_keys=c.ssh_public_keys,
                )
            ]

        self.pipeline = databricks.Pipeline(
                f"pipline-{pipeline.name}",
                allow_duplicate_names=pipeline.allow_duplicate_names,
                catalog=pipeline.catalog,
                channel=pipeline.channel,
                clusters=clusters,
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

        # Pipline configuration
        filepath = "./tmp.json"
        s = pipeline.model_dump_json(indent=4)
        with open(filepath, "w") as fp:
            fp.write(s)
        self.conf = databricks.WorkspaceFile(
            f"file-{pipeline.name}",
            path=f"/.laktory/pipelines/{pipeline.name}.json",
            # md5=hashlib.md5(s.encode('utf-8')).hexdigest(),
            source=filepath,
            opts=opts,
        )
        os.remove(filepath)

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
