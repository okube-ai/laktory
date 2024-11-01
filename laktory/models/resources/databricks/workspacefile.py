import os
from pathlib import Path
from typing import Any
from typing import Union
from pydantic import model_validator
from laktory import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions


class WorkspaceFile(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Workspace File

    Attributes
    ----------
    access_controls:
        List of file access controls
    rootpath:
        Root directory to which all workspace files are deployed to. Can also
        be configured by settings LAKTORY_WORKSPACE_LAKTORY_ROOT environment
        variable. Default is `/.laktory/`. Used only if `path` is not
        specified.
    dirpath:
        Workspace directory inside rootpath in which the workspace file is
        deployed. Used only if `path` is not specified.
    path:
         Workspace filepath for the file
    source:
        Path to file on local filesystem.

    Examples
    --------
    ```py
    from laktory import models

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
    )
    print(file.path)
    #> /.laktory/dlt_laktory_pl.py

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        rootpath="/src/",
    )
    print(file.path)
    #> /src/dlt_laktory_pl.py

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        rootpath="/src/",
        dirpath="notebooks/dlt/",
    )
    print(file.path)
    #> /src/notebooks/dlt/dlt_laktory_pl.py
    ```
    """

    access_controls: list[AccessControl] = []
    rootpath: str = None
    dirpath: str = None
    path: str = None
    source: str

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {"path": ""}

    @property
    def filename(self) -> str:
        """File filename"""
        return os.path.basename(self.source)

    @model_validator(mode="after")
    def set_rootpath(self) -> Any:
        if self.path is None and self.rootpath is None:
            self.rootpath = settings.workspace_laktory_root
        return self

    @model_validator(mode="after")
    def set_dirpath(self) -> Any:
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]
        return self

    @model_validator(mode="after")
    def set_path(self) -> Any:
        if self.path is None:
            _path = Path(self.rootpath) / self.dirpath / self.filename
            self.path = _path.as_posix()
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """path with special characters `/`, `.`, `\\` replaced with `-`"""
        # key = os.path.splitext(self.path)[0]
        key = self.path
        key = key.replace("/", "-")
        key = key.replace("\\", "-")
        key = key.replace(".", "-")
        for i in range(5):
            if key.startswith("-"):
                key = key[1:]
        return key

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
        return ["access_controls", "rootpath", "dirpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_file"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
