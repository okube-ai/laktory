import os
from typing import Any
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission


class WorkspaceFile(BaseModel, BaseResource):
    """
    Databricks Workspace File

    Attributes
    ----------
    source:
        Path to file on local filesystem.
    dirpath:
        Workspace directory containing the file. Filename will be assumed to be the same as local filepath. Used if path
        is not specified.
    path:
         Workspace filepath for the file
    permissions:
        List of file permissions
    """

    source: str
    dirpath: str = None
    path: str = None
    permissions: list[Permission] = []

    @property
    def filename(self) -> str:
        """File filename"""
        return os.path.basename(self.source)

    @model_validator(mode="after")
    def default_path(self) -> Any:
        if self.path is None:
            if self.dirpath:
                self.path = f"{self.dirpath}{self.filename}"

            elif "/workspacefiles/" in self.source:
                self.path = "/" + self.source.split("/workspacefiles/")[-1]

            else:
                raise ValueError(
                    "A value for `dirpath` must be specified if the source is not in a `workspacefiles` folder"
                )

        return self

    @property
    def resource_key(self) -> str:
        """File resource key"""
        key = os.path.splitext(self.path)[0].replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        return key

    # resource_key.__doc__ = super().resource_key.__doc__

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "dirpath"]

    def deploy_with_pulumi(self, name=None, opts=None):
        """
        Deploy workspace file using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiNotebook:
            Pulumi workspace file resource
        """
        from laktory.resourcesengines.pulumi.workspacefile import PulumiWorkspaceFile

        return PulumiWorkspaceFile(name=name, workspace_file=self, opts=opts)
