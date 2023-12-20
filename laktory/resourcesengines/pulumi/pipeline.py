import json
import pulumi
import pulumi_databricks as databricks

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.databricks.pipeline import Pipeline
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine

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
            name = pipeline.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        # ------------------------------------------------------------------- #
        # Pipeline                                                            #
        # ------------------------------------------------------------------- #

        self.pipeline = databricks.Pipeline(
            name,
            opts=opts,
            **pipeline.model_pulumi_dump(),
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
                f"permissions-{name}",
                access_controls=access_controls,
                pipeline_id=self.pipeline.id,
                opts=opts,
            )

        # ------------------------------------------------------------------- #
        # Pipeline Configuration                                              #
        # ------------------------------------------------------------------- #

        source = f"./tmp-{pipeline.name}.json"
        d = pipeline.model_dump(exclude_none=True)
        d = pipeline.inject_vars(d)
        s = json.dumps(d, indent=4)
        with open(source, "w", newline="\n") as fp:
            fp.write(s)
        filepath = f"{settings.workspace_laktory_root}pipelines/{pipeline.name}.json"
        self.workspace_file = databricks.WorkspaceFile(
            f"file-{filepath}",
            path=filepath,
            # md5=hashlib.md5(s.encode('utf-8')).hexdigest(),
            source=source,
            opts=opts,
        )
        # os.remove(filepath)

        _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.workspace_file))
        if access_controls:
            self.conf_permissions = databricks.Permissions(
                f"permissions-file-{filepath}",
                access_controls=[
                    databricks.PermissionsAccessControlArgs(
                        permission_level="CAN_READ",
                        group_name="account users",
                    )
                ],
                workspace_file_path=filepath,
                opts=opts,
            )
