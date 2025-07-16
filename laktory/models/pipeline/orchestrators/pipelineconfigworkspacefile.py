import base64
import json

from laktory._settings import settings
from laktory.models.pipelinechild import PipelineChild
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.workspacefile import WorkspaceFile


class PipelineConfigWorkspaceFile(WorkspaceFile, PipelineChild):
    """
    Workspace File storing pipeline configuration. Default values for path and
    access controls. Forced value for source.
    """

    access_controls: list[AccessControl] = [
        AccessControl(permission_level="CAN_READ", group_name="users")
    ]

    @property
    def path_(self):
        pl = self.parent_pipeline
        if not pl:
            return None

        return pl.orchestrator.config_file_path

    @property
    def content_dict(self):
        pl = self.parent_pipeline
        if not pl:
            return None

        pl = pl.inject_vars()

        # Overwrite serialization options
        ss0 = self._singular_serialization
        cs0 = self._camel_serialization
        #
        # # Inject variables before serialization (in case computed fields hashes
        # # other field values)
        # o = _pl.orchestrator
        # _pl.orchestrator = None
        # _pl = _pl.inject_vars()
        # # _pl.orchestrator = None
        # # _pl = _pl.inject_vars()
        #
        # _config = _pl.model_dump(exclude_unset=True,  mode="json")
        # _config["orchestrator"] = o.model_dump(exclude_unset=True,  mode="json")

        # Reset serialization options
        pl._configure_serializer(singular=ss0, camel=cs0)

        _confg = {}

        return _config

    @property
    def content_base64_(self):
        _config = self.content_dict
        _config_str = json.dumps(_config, indent=4)
        return base64.b64encode(_config_str.encode("utf-8")).decode("utf-8")

    def update_from_parent(self):
        """
        Path is required to be set here (after instantiation). Other resource key is not
        defined and resources are not created properly.
        """
        pl = self.parent_pipeline
        if not pl:
            return

        # Set path
        self.path = self.path_

        # Mock content
        self.content_base64 = base64.b64encode("<place_holder>".encode("utf-8")).decode(
            "utf-8"
        )

    def _post_serialization(self, dump):
        """
        Content is required to be set here (at serialization). Otherwise, it leas to
        infinite lops.
        """
        dump["content_base64"] = self.content_base64_
        return dump

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str] | dict[str, bool]:
        return super().pulumi_excludes + [
            "dataframe_backend",
            "dataframe_api",
        ]

    @property
    def resource_type_id(self):
        return "workspace-file"
