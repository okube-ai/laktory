import os
from pathlib import Path

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory.models.resources.databricks._renderablefile import RenderableFileMixin
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.workspacefile_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacefile_base import WorkspaceFileBase


class WorkspaceFile(RenderableFileMixin, WorkspaceFileBase):
    """
    Databricks Workspace File

    Examples
    --------
    ```py
    import io

    from laktory import models

    file_yaml = '''
    source: ./notebooks/dlt/dlt_laktory_pl.py
    dirpath: notebooks/dlt/
    '''
    file = models.resources.databricks.WorkspaceFile.model_validate_yaml(
        io.StringIO(file_yaml)
    )
    print(file.path)
    # > /.laktory/notebooks/dlt/dlt_laktory_pl.py
    ```

    References
    ----------

    * [Databricks Workspace File](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/workspace_file)
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the workspace file is deployed. Used only if `path` is not specified.",
    )
    path_: str = Field(
        None,
        description="Workspace filepath for the file. Overwrite `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )
    source_: str = Field(
        None,
        description="Path to file on local filesystem.",
        validation_alias=AliasChoices("source", "source_"),
        exclude=True,
    )

    @computed_field(description="source")
    @property
    def source(self) -> str:
        if self.render_vars:
            return self._staged_path("workspace_files", self.path)
        return self.source_

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_

        if not self.source_:
            return None

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        path = Path(settings.workspace_root) / self.dirpath / self.filename
        return path.as_posix()

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {"path": ""}

    @property
    def filename(self) -> str | None:
        """File filename"""
        if self.source_:
            return os.path.basename(self.source_)

    def build(self, vars: dict = None):
        """
        Render `${vars.x}`/`${{ expr }}` placeholders in the file content
        and stage the result under `settings.build_root`, if `render_vars`
        is `True`. No-op otherwise.
        """
        if self.render_vars:
            self._render_to_staged_path(self.source_, self.source, vars=vars)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.path

    @property
    def additional_core_resources(self) -> list:
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    # workspace_file_path=f"${{resources.{self.resource_name}.path}}",
                    workspace_file_path=self.path,
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls", "dirpath", "render_vars"]
