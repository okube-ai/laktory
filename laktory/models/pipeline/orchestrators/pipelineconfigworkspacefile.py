import json
from pathlib import Path

from pydantic import computed_field

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

    @computed_field(description="source")
    @property
    def source(self) -> str | None:
        from laktory._cache import cache_dir

        pl_name = ""
        try:
            pl = self.parent_pipeline
            pl_name = pl.name
        except ImportError:
            # parent pipeline can't be access at initial import
            pass

        source_path = cache_dir / "pipelines" / pl_name / "config.json"

        return str(source_path)

    @property
    def path_(self):
        if self.path:
            return self.path

        pl = self.parent_pipeline
        if not pl:
            return None

        return f"{settings.workspace_laktory_root}pipelines/{pl.name}/config.json"

    @property
    def content_dict(self):
        pl = self.parent_pipeline
        if not pl:
            return None

        # Overwrite serialization options
        ss0 = self._singular_serialization
        cs0 = self._camel_serialization
        pl._configure_serializer(singular=False, camel=False)

        # Orchestrator (which includes WorkspaceFile) needs to be excluded to avoid
        # infinite re-cursive loop
        _config = self.inject_vars_into_dump(
            {
                "config": pl.model_dump(
                    exclude_unset=True, exclude="orchestrator", mode="json"
                )
            }
        )["config"]
        _config["orchestrator"] = pl.orchestrator.model_dump(
            exclude_unset=True, exclude="config_file", mode="json"
        )

        # Reset serialization options
        pl._configure_serializer(singular=ss0, camel=cs0)

        return _config

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

    def _post_serialization(self, dump):
        """
        Content is required to be set here (at serialization). Otherwise, it leas to
        infinite lops.
        """
        filepath = Path(self.source)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as fp:
            json.dump(self.content_dict, fp, indent=4)
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
