import os
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
    dirpath:
        Workspace directory containing the file. Filename will be assumed to be the same as local filepath. Used if path
        is not specified.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    path:
         DBFS filepath for the file
    source:
        Path to file on local filesystem.
    """

    access_controls: list[AccessControl] = []
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
    def default_path(self) -> Any:
        if self.path is None:
            if self.dirpath:
                self.path = f"{self.dirpath}{self.filename}"
            else:
                raise ValueError(
                    "A value for `dirpath` must be specified if `path` is not specified"
                )

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
