from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.workspacefile import WorkspaceFile

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiWorkspaceFile(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        workspace_file: WorkspaceFile = None,
        opts=None,
    ):
        if name is None:
            name = f"workspace-file-{workspace_file.key}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.file = databricks.WorkspaceFile(
            f"workspace-file-{workspace_file.key}",
            opts=opts,
            **workspace_file.model_pulumi_dump(),
        )

        access_controls = []
        for permission in workspace_file.permissions:
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
                f"permissions-file-{workspace_file.key}",
                access_controls=access_controls,
                workspace_file_path=self.file.path,
                opts=opts,
            )
