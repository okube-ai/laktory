from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class Directory(BaseModel, PulumiResource):
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

    d = models.Directory(path="/queries/views")
    print(d)
    #> vars={} path='/queries/views' delete_recursive=None
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
        """Key identifier for the directory"""
        key = self.path.replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
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

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks
        return databricks.Directory
