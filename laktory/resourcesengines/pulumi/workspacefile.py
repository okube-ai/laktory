from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.databricks.workspacefile import WorkspaceFile

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
            name = workspace_file.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.file = databricks.WorkspaceFile(
            name,
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

        _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.file))
        if access_controls:
            self.permissions = databricks.Permissions(
                f"permissions-file-{workspace_file.resource_key}",
                access_controls=access_controls,
                workspace_file_path=self.file.path,
                opts=_opts,
            )
