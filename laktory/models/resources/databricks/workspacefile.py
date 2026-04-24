import os
from pathlib import Path
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.workspacefile_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacefile_base import WorkspaceFileBase
from laktory.models.resources.pulumiresource import PulumiResource


class WorkspaceFile(WorkspaceFileBase, PulumiResource):
    """
    Databricks Workspace File

    Examples
    --------
    ```py
    from laktory import models

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
    )
    print(file.path)
    # > /.laktory/dlt_laktory_pl.py

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
    )
    print(file.path)
    # > /.laktory/dlt_laktory_pl.py

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        dirpath="notebooks/dlt/",
    )
    print(file.path)
    # > /.laktory/notebooks/dlt/dlt_laktory_pl.py
    ```
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
        if self.source:
            return os.path.basename(self.source)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.path

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    # workspace_file_path=f"${{resources.{self.resource_name}.path}}",
                    workspace_file_path=self.path,
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:WorkspaceFile"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "dirpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
