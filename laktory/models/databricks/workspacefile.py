import os
from typing import Any
from typing import Union
from pydantic import model_validator
from laktory import constants
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.accesscontrol import AccessControl
from laktory.models.databricks.permissions import Permissions


class WorkspaceFile(BaseModel, PulumiResource):
    """
    Databricks Workspace File

    Attributes
    ----------
    access_controls:
        List of file access controls
    dirpath:
        Workspace directory containing the file. Filename will be assumed to be the same as local filepath. Used if path
        is not specified.
    path:
         Workspace filepath for the file
    source:
        Path to file on local filesystem.
    """

    access_controls: list[AccessControl] = []
    dirpath: str = None
    path: str = None
    source: str

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
                self.path = (
                    constants.LAKTORY_WORKSPACE_ROOT
                    + self.source.split("/workspacefiles/")[-1]
                )

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
        # key = os.path.splitext(self.path)[0]
        key = self.path
        key = key.replace("/", "-")
        key = key.replace("\\", "-")
        key = key.replace(".", "-")
        for i in range(5):
            if key.startswith("-"):
                key = key[1:]
        return key

    @property
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [
                self,
            ]
            if self.access_controls:
                self.resources_ += [
                    Permissions(
                        resource_name=f"permissions-{self.resource_name}",
                        access_controls=self.access_controls,
                        workspace_file_path=self.path,
                        options={
                            "depends_on": [f"${{resources.{self.resource_name}}}"]
                        },
                    )
                ]

        return self.resources_

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
        return ["access_controls", "dirpath"]
