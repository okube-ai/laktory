import os
import json

from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.databricks.accesscontrol import AccessControl


class PipelineConfigWorkspaceFile(WorkspaceFile, PipelineChild):
    """
    Workspace File storing pipeline configuration. Default values for path and
    access controls. Forced value for source.

    Attributes
    ----------
    access_controls:
        List of file access controls
    path:
         Workspace filepath for the file. Overwrite `rootpath` and `dirpath`.
         Default value `{settings.workspace_laktory_root}pipelines/{pl_name}/config.json`
    """

    source: str = "{pl_name}"
    access_controls: list[AccessControl] = [
        AccessControl(permission_level="CAN_READ", group_name="users")
    ]

    def update_from_parent(self):

        pl = self.parent_pipeline
        pl_name = pl.name
        self.source = os.path.join(CACHE_ROOT, f"tmp-{pl_name}-config.json")
        if "{pl_name}" in self.path:
            self.path = (
                f"{settings.workspace_laktory_root}pipelines/{pl_name}/config.json"
            )
        self.set_paths()

    def write_source(self):

        pl = self.parent_pipeline

        pl.root_path = pl._root_path.as_posix()
        pl = pl.inject_vars(inplace=False)

        d = pl.model_dump(exclude_unset=True)
        s = json.dumps(d, indent=4)

        source = self.inject_vars_into_dump({"source": self.source})["source"]
        with open(source, "w", newline="\n") as fp:
            fp.write(s)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self):
        return "workspace-file"
