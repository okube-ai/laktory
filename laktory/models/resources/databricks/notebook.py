import os
from pathlib import Path
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory.models.resources.baseresource import ResourceLookup
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


class Notebook(NotebookBase):
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
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    path_: str = Field(
        None,
        description="Workspace filepath for the notebook. Overwrite `rootpath` and `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        path = Path(settings.workspace_root) / self.dirpath / self.filename
        return path.as_posix()

    @property
    def filename(self) -> str:
        """Notebook file name"""
        if self.source is None:
            return ""
        return os.path.basename(self.source)

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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "dirpath"]
