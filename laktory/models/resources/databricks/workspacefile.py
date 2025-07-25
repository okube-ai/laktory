import os
from pathlib import Path
from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceFile(BaseModel, PulumiResource, TerraformResource):
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
        rootpath="/src/",
    )
    print(file.path)
    # > /src/dlt_laktory_pl.py

    file = models.resources.databricks.WorkspaceFile(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        rootpath="/src/",
        dirpath="notebooks/dlt/",
    )
    print(file.path)
    # > /src/notebooks/dlt/dlt_laktory_pl.py
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the workspace file is deployed. Used only if `path` is not specified.",
    )
    path: str = Field(
        None,
        description="Workspace filepath for the file. Overwrite `rootpath` and `dirpath`.",
    )
    rootpath: str = Field(
        None,
        description="""
    Root directory to which all workspace files are deployed to. Can also be configured by settings 
    LAKTORY_WORKSPACE_LAKTORY_ROOT environment variable. Default is `/.laktory/`. Used only if `path` is not specified.
    """,
    )
    source: str = Field(None, description="Path to file on local filesystem.")
    content_base64: str = Field(
        None,
        description="""
    The base64-encoded file content. Conflicts with source. Use of content_base64 is discouraged, as it's increasing 
    memory footprint of Pulumi state and should only be used in exceptional circumstances, like creating a workspace 
    file with configuration properties for a data pipeline.
    """,
    )

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {"path": ""}

    @property
    def filename(self) -> str:
        """File filename"""
        if self.source:
            return os.path.basename(self.source)

    @model_validator(mode="after")
    def set_paths(self) -> Any:
        # Path set
        if self.path:
            return self

        if not self.source:
            return self

        # root
        if self.rootpath is None:
            self.rootpath = settings.workspace_laktory_root

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        # path
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
        return ["access_controls", "dirpath", "rootpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_file"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
