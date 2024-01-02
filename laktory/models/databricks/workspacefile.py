import os
from typing import Any
from typing import Union
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.permission import Permission
from laktory.models.databricks.permissions import Permissions


class WorkspaceFile(BaseModel, PulumiResource):
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


    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """File resource key"""
        key = os.path.splitext(self.path)[0].replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        return key

    @property
    def all_resources(self) -> list[PulumiResource]:
        res = [
            self,
        ]
        if self.permissions:

            # TODO: _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.file))
            res += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.permissions,
                    workspace_file_path=self.path,
                )
            ]

        return res

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:WorkspaceFile"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks
        return databricks.WorkspaceFile

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["permissions", "dirpath"]
