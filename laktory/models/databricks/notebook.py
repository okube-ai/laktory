import os
from typing import Any
from typing import Literal
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission


class Notebook(BaseModel, BaseResource):
    """
    Databricks Notebook

    Attributes
    ----------
    source:
        Path to notebook in source code format on local filesystem.
    dirpath:
        Workspace directory containing the notebook. Filename will be assumed to be the same as local filepath. Used
        if path is not specified.
    path:
        Workspace filepath for the notebook
    language:
         Notebook programming language
    permissions:
        List of notebook permissions

    Examples
    --------
    ```py
    from laktory import models

    notebook = models.Notebook(
        source="./notebooks/pipelines/dlt_brz_template.py",
    )
    print(notebook.path)
    #> /pipelines/dlt_brz_template.py

    notebook = models.Notebook(source="./notebooks/create_view.py", dirpath="/views/")
    print(notebook.path)
    #> /views/create_view.py
    ```
    """

    source: str
    dirpath: str = None
    path: str = None
    language: Literal["SCALA", "PYTHON", "SQL", "R"] = None
    permissions: list[Permission] = []

    @property
    def filename(self) -> str:
        """Notebook file name"""
        return os.path.basename(self.source)

    @model_validator(mode="after")
    def default_path(self) -> Any:
        if self.path is None:
            if self.dirpath:
                self.path = f"{self.dirpath}{self.filename}"

            elif "/notebooks/" in self.source:
                self.path = "/" + self.source.split("/notebooks/")[-1]

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Notebook resource key"""
        key = os.path.splitext(self.path)[0].replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        return key

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "dirpath"]

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        """
        Deploy notebook using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiNotebook:
            Pulumi notebook resource
        """
        from laktory.resourcesengines.pulumi.notebook import PulumiNotebook

        return PulumiNotebook(name=name, notebook=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    notebook = models.Notebook(
        source="./notebooks/pipelines/dlt_brz_template.py",
    )
    print(notebook.path)
    # > /pipelines/dlt_brz_template.py

    notebook = models.Notebook(source="./notebooks/create_view.py", dirpath="/views/")
    print(notebook.path)
    # > /views/create_view.py
