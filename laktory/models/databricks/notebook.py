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
        Workspace directory containing the notebook. Filename will be assumed
        to be the same as local filepath.
    language:
         Notebook programming language
    permissions:
        List of notebook permissions
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

    @property
    def key(self) -> str:
        """Notebook resource key"""
        key = os.path.splitext(self.path)[0].replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        return key

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
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "dirpath"]

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        """
        Deploy notebook using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `notebook-{self.key}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiNotebook:
            Pulumi notebook resource
        """
        from laktory.resourcesengines.pulumi.notebook import PulumiNotebook

        return PulumiNotebook(name=name, notebook=self, opts=opts)
