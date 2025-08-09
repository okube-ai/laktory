import os
from pathlib import Path
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class DbfsFileLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id",
        description="Path on DBFS for the file from which to get content.",
    )
    limit_file_size: bool = Field(
        True, description="Do not load content for files larger than 4MB."
    )


class DbfsFile(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks DBFS File

    Examples
    --------
    ```py
    from laktory import models

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
    )
    print(file.path)
    # > /prices.json

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
    )
    print(file.path)
    # > /prices.json

    file = models.resources.databricks.DbfsFile(
        source="./data/stock_prices/prices.json",
        dirpath="stock_prices/",
    )
    print(file.path)
    # > /stock_prices/prices.json
    ```
    """

    access_controls: list[AccessControl] = Field(
        [], description="List of file access controls"
    )
    dirpath: str = Field(
        None,
        description="""
    Workspace directory inside rootpath in which the DBFS file is deployed. Used only if `path` is not specified.
    """,
    )
    lookup_existing: DbfsFileLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    path_: str = Field(
        None,
        description="DBFS filepath for the file. Overwrite `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )
    source: str = Field(..., description="Path to file on local filesystem.")

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

        path = Path("/") / self.dirpath / self.filename
        return path.as_posix()

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {"path": ""}

    @property
    def filename(self) -> str:
        """File filename"""
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
        return ["access_controls", "dirpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dbfs_file"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
