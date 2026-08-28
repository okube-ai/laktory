import os
from pathlib import Path

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks._renderablefile import RenderableFileMixin
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.notebook_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.notebook_base import NotebookBase
from laktory.models.resources.databricks.permissions import Permissions


class NotebookLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id", description="Notebook path on the workspace"
    )
    format: str = Field(
        "SOURCE",
        description="Notebook format to export. Either `SOURCE`, `HTML`, `JUPYTER`, or `DBC`",
    )


class Notebook(RenderableFileMixin, NotebookBase):
    """
    Databricks Notebook

    Examples
    --------
    ```py
    import io

    from laktory import models

    notebook_yaml = '''
    source: ./notebooks/dlt/dlt_laktory_pl.py
    dirpath: notebooks/dlt/
    access_controls:
    - group_name: role-engineers
      permission_level: CAN_RUN
    '''
    notebook = models.resources.databricks.Notebook.model_validate_yaml(
        io.StringIO(notebook_yaml)
    )
    print(notebook.path)
    # > /.laktory/notebooks/dlt/dlt_laktory_pl.py
    ```

    References
    ----------

    * [Databricks Notebook](https://docs.databricks.com/en/notebooks/index.html)
    """

    access_controls: list[AccessControl] = Field(
        [], description="List of notebook access controls"
    )
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the notebook is deployed. Used only if `path` is not specified.",
    )
    lookup_existing: NotebookLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Notebook by `path` instead of creating it. The notebook becomes available for cross-referencing; its own field values are not written to the existing resource.",
    )
    path_: str = Field(
        None,
        description="Workspace filepath for the notebook. Overwrite `rootpath` and `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )
    source_: str = Field(
        None,
        description="Path to notebook in source code format on local filesystem. Conflicts with `content_base64`",
        validation_alias=AliasChoices("source", "source_"),
        exclude=True,
    )

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_

        # dir - normalize a native Windows separator (e.g. a backslash-prefixed
        # dirpath) so `Path(...) / self.dirpath` below can't treat it as an
        # absolute anchor and silently drop `settings.workspace_root`
        if self.dirpath is None:
            self.dirpath = ""
        self.dirpath = self.dirpath.replace("\\", "/")
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        path = Path(settings.workspace_root) / self.dirpath / self.filename
        return path.as_posix()

    @computed_field(description="source")
    @property
    def source(self) -> str | None:
        if self.render_vars:
            return self._staged_path("workspace_files", self.path)
        return self.source_

    @property
    def filename(self) -> str:
        """Notebook file name"""
        if self.source_ is None:
            return ""
        return os.path.basename(self.source_)

    def build(self, vars: dict = None):
        """
        Render `${vars.x}`/`${{ expr }}` placeholders in the notebook
        content and stage the result under `settings.build_root`, if
        `render_vars` is `True`. No-op otherwise.
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
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    notebook_path=f"${{resources.{self.resource_name}.path}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls", "dirpath", "render_vars"]
