from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Directory(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Directory

    Attributes
    ----------
    path:
        The absolute path of the directory, beginning with "/", e.g. "/pipelines".
    delete_recursive:
        When `True`, subdirectories are also deleted when the directory is deleted

    Examples
    --------
    ```py
    from laktory import models

    d = models.resources.databricks.Directory(path="/queries/views")
    print(d)
    '''
    resource_name_=None options=ResourceOptions(variables={}, depends_on=[], provider=None, aliases=None, delete_before_replace=True, ignore_changes=None, import_=None, parent=None, replace_on_changes=None) variables={} path='/queries/views' delete_recursive=None
    '''
    print(d.resource_key)
    #> queries-views
    print(d.resource_name)
    #> directory-queries-views
    ```
    """

    path: str
    delete_recursive: Union[bool, None] = None

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
