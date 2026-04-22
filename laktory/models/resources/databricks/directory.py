from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.directory_base import DirectoryBase
from laktory.models.resources.pulumiresource import PulumiResource


class DirectoryLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id",
        description="The absolute path of the directory, beginning with '/', e.g. '/Demo'.",
    )


class Directory(DirectoryBase, PulumiResource):
    """
    Databricks Directory

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Directory(path="/queries/views")
    print(d)
    '''
    resource_name_=None options=ResourceOptions(variables={}, is_enabled=True, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None, moved_from=None) lookup_existing=None variables={} path='/queries/views' delete_recursive=None object_id=None
    '''
    print(d.resource_key)
    # > /queries/views
    print(d.resource_name)
    # > directory-queries-views
    ```
    """

    lookup_existing: DirectoryLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )

    @property
    def resource_key(self) -> str:
        """path with special characters `/`, `.`, `\\` replaced with `-`"""
        return self.path

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
