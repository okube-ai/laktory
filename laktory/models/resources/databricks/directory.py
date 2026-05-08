from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.directory_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.directory_base import DirectoryBase


class DirectoryLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id",
        description="The absolute path of the directory, beginning with '/', e.g. '/Demo'.",
    )


class Directory(DirectoryBase):
    """
    Databricks Directory

    Examples
    --------
    ```py
    import io

    from laktory import models

    dir_yaml = '''
    path: /queries/views
    '''
    d = models.resources.databricks.Directory.model_validate_yaml(io.StringIO(dir_yaml))
    print(d.resource_key)
    # > /queries/views
    print(d.resource_name)
    # > directory-queries-views
    ```

    References
    ----------

    * [Databricks Directory](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/directory)
    """

    lookup_existing: DirectoryLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing workspace Directory by `path` instead of creating it. The directory becomes available for cross-referencing; its own field values are not written to the existing resource.",
    )

    @property
    def resource_key(self) -> str:
        """path with special characters `/`, `.`, `\\` replaced with `-`"""
        return self.path

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
