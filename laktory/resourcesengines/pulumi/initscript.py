from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.initscript import InitScript

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiInitScript(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            init_script: InitScript = None,
            opts=None,
    ):
        if name is None:
            name = f"init-script-{init_script.key}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.file = databricks.WorkspaceFile(
                f"file-{init_script.key}",
                path=init_script.path,
                source=init_script.source,
                opts=opts,
            )

        access_controls = []
        for permission in init_script.permissions:
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
                f"permissions-file-{init_script.key}",
                access_controls=access_controls,
                workspace_file_path=self.file.path,
                opts=opts,
            )
