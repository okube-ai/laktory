import os
from pathlib import Path
from typing import Literal
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class NotebookLookup(ResourceLookup):
    path: str = Field(
        serialization_alias="id", description="Notebook path on the workspace"
    )
    format: str = Field(
        "SOURCE",
        description="Notebook format to export. Either `SOURCE`, `HTML`, `JUPYTER`, or `DBC`",
    )


class Notebook(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Notebook

    Examples
    --------
    ```py
    from laktory import models

    notebook = models.resources.databricks.Notebook(
        source="./notebooks/dlt/dlt_laktory_pl.py",
    )
    print(notebook.path)
    # > /.laktory/dlt_laktory_pl.py

    notebook = models.resources.databricks.Notebook(
        source="./notebooks/dlt/dlt_laktory_pl.py",
    )
    print(notebook.path)
    # > /.laktory/dlt_laktory_pl.py

    notebook = models.resources.databricks.Notebook(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        dirpath="notebooks/dlt/",
    )
    print(notebook.path)
    # > /.laktory/notebooks/dlt/dlt_laktory_pl.py
    ```
    """

    access_controls: list[AccessControl] = Field(
        [], description="List of notebook access controls"
    )
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the notebook is deployed. Used only if `path` is not specified.",
    )
    language: Literal["SCALA", "PYTHON", "SQL", "R"] = Field(
        None, description="Notebook programming language"
    )
    lookup_existing: NotebookLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    path_: str = Field(
        None,
        description="Workspace filepath for the notebook. Overwrite `rootpath` and `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )
    source: str = Field(
        ..., description="Path to notebook in source code format on local filesystem."
    )

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        path = Path(settings.workspace_laktory_root) / self.dirpath / self.filename
        return path.as_posix()

    @property
    def filename(self) -> str:
        """Notebook file name"""
        return os.path.basename(self.source)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.path

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    notebook_path=f"${{resources.{self.resource_name}.path}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Notebook"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "dirpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_notebook"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
