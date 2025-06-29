from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class DirectoryLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id",
        description="The absolute path of the directory, beginning with '/', e.g. '/Demo'.",
    )


class Directory(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Directory

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Directory(path="/queries/views")
    print(d)
    '''
    resource_name_=None options=ResourceOptions(variables={}, is_enabled=True, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None, moved_from=None) lookup_existing=None variables={} delete_recursive=None path='/queries/views'
    '''
    print(d.resource_key)
    # > queries-views
    print(d.resource_name)
    # > directory-queries-views
    ```
    """

    delete_recursive: Union[bool, None] = Field(
        None,
        description=" When `True`, subdirectories are also deleted when the directory is deleted",
    )
    lookup_existing: DirectoryLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    path: str = Field(
        ...,
        description="The absolute path of the directory, beginning with '/', e.g. '/pipelines'.",
    )

    @property
    def resource_key(self) -> str:
        """path with special characters `/`, `.`, `\\` replaced with `-`"""
        key = self.path
        key = key.replace("/", "-")
        key = key.replace("\\", "-")
        key = key.replace(".", "-")
        for i in range(5):
            if key.startswith("-"):
                key = key[1:]
        for i in range(5):
            if key.endswith("-"):
                key = key[:-1]
        return key

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Directory"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_directory"
