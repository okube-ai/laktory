import os
from pathlib import Path
from typing import Any
from typing import Union
from pydantic import model_validator
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions


class DbfsFileLookup(ResourceLookup):
    """
    Attributes
    ----------
    path:
        Path on DBFS for the file from which to get content.
    limit_file_size:
        Do not load content for files larger than 4MB.
    """

    path: str = Field(serialization_alias="id")
    limit_file_size: bool = True


class DbfsFile(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks DBFS File

    Attributes
    ----------
    access_controls:
        List of file access controls
    rootpath:
        Root directory to which all DBFS files are deployed to. Used only if
        `path` is not specified.
    dirpath:
        Workspace directory inside rootpath in which the DBFS file is
        deployed. Used only if `path` is not specified.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    path:
         DBFS filepath for the file
    source:
        Path to file on local filesystem.

    Examples
    --------
    ```py
    from laktory import models

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
    )
    print(file.path)
    #> /prices.json

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
        rootpath="/data/",
    )
    print(file.path)
    #> /data/prices.json

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
        rootpath="/data/",
        dirpath="stock_prices/",
    )
    print(file.path)
    #> /data/stock_prices/prices.json
    ```
    """

    access_controls: list[AccessControl] = []
    rootpath: str = "/"
    dirpath: str = None
    lookup_existing: DbfsFileLookup = Field(None, exclude=True)
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
                    workspace_file_path=f"${{resources.{self.resource_name}.path}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:DbfsFile"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "rootpath", "dirpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dbfs_file"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
