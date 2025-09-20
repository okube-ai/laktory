import json
from pathlib import Path

from pydantic import computed_field

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.pipelinechild import PipelineChild
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.workspacefile import WorkspaceFile

logger = get_logger(__name__)


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

        source_path = cache_dir / "pipelines" / (pl_name + ".json")

        return str(source_path)

    @computed_field(description="source")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_

        pl = self.parent_pipeline
        if not pl:
            return None

        return f"{settings.workspace_laktory_root}pipelines/{pl.name}.json"

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

    def build(self):
        """
        Write config file to cache (required for deployment).
        """
        filepath = Path(self.source)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Writing config file at {filepath}")
        with filepath.open(mode="w") as fp:
            json.dump(self.content_dict, fp, indent=4)

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
