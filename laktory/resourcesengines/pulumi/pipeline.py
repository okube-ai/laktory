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

        # TODO:
        clusters = []

        libraries = []
        for l in pipeline.libraries:
            if l.file:
                libraries += [databricks.PipelineLibraryArgs(
                    file=databricks.PipelineLibraryFileArgs(path=l.file.path)
                )]
            if l.notebook:
                libraries += [databricks.PipelineLibraryArgs(
                    notebook=databricks.PipelineLibraryFileArgs(path=l.notebook.path)
                )]

        # TODO:
        notifications = []

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
                libraries=libraries,
                name=pipeline.name,
                notifications=notifications,
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
