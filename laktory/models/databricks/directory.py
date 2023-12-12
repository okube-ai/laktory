from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource


class Directory(BaseModel, BaseResource):
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
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def id(self):
        if self._resources is None:
            return None
        return self.resources.directory.id

    @property
    def object_id(self):
        if self._resources is None:
            return None
        return self.resources.directory.object_id

    def deploy_with_pulumi(self, name: str = None, opts=None):
        """
        Deploy directory using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiDirectory:
            Pulumi directory resource
        """
        from laktory.resourcesengines.pulumi.directory import PulumiDirectory

        return PulumiDirectory(name=name, directory=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    d = models.Directory(path="/queries/views")
    print(d)
    print(d.key)
