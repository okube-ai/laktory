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
        pl = self.parent_pipeline
        if not pl:
            return

        # Set path
        if self.path is None:
            self.path = (
                f"{settings.workspace_laktory_root}pipelines/{pl.name}/config.json"
            )

        _config = self.inject_vars_into_dump(
            {"config": pl.model_dump(exclude_unset=True)}
        )["config"]

        _config_str = json.dumps(_config, indent=4)
        b64_str = base64.b64encode(_config_str.encode("utf-8")).decode("utf-8")

        self.content_base64 = b64_str

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self):
        return "workspace-file"
