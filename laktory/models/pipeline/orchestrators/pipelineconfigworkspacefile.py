import base64
import json

from laktory._settings import settings
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.workspacefile import WorkspaceFile


class PipelineConfigWorkspaceFile(WorkspaceFile, PipelineChild):
    """
    Workspace File storing pipeline configuration. Default values for path and
    access controls. Forced value for source.

    Parameters
    ----------
    access_controls:
        List of file access controls
    """

    access_controls: list[AccessControl] = [
        AccessControl(permission_level="CAN_READ", group_name="users")
    ]

    @property
    def path_(self):
        if self.path:
            return self.path

        pl = self.parent_pipeline
        if not pl:
            return None

        return f"{settings.workspace_laktory_root}pipelines/{pl.name}/config.json"

    @property
    def content_base64_(self):
        if self.content_base64:
            return self.content_base64

        pl = self.parent_pipeline
        if not pl:
            return None

        _config = self.inject_vars_into_dump(
            {"config": pl.model_dump(exclude_unset=True, exclude="orchestrator")}
        )["config"]
        _config_str = json.dumps(_config, indent=4)
        return base64.b64encode(_config_str.encode("utf-8")).decode("utf-8")

    def update_from_parent(self):
        # Set path
        if self.path_:
            self.path = self.path_

        # Set content
        if self.content_base64_:
            self.content_base64 = self.content_base64_

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self):
        return "workspace-file"
