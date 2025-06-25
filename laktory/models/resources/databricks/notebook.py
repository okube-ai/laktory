import os
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

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
        rootpath="/src/",
    )
    print(notebook.path)
    # > /src/dlt_laktory_pl.py

    notebook = models.resources.databricks.Notebook(
        source="./notebooks/dlt/dlt_laktory_pl.py",
        rootpath="/src/",
        dirpath="notebooks/dlt/",
    )
    print(notebook.path)
    # > /src/notebooks/dlt/dlt_laktory_pl.py
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
    path: str = Field(
        None,
        description="Workspace filepath for the notebook. Overwrite `rootpath` and `dirpath`.",
    )
    rootpath: str = Field(
        None,
        description="""
    Root directory to which all notebooks are deployed to. Can also be configured by settings 
    LAKTORY_WORKSPACE_LAKTORY_ROOT environment variable. Default is `/.laktory/`. Used only if `path` is not specified.
    """,
    )
    source: str = Field(
        ..., description="Path to notebook in source code format on local filesystem."
    )

    @property
    def filename(self) -> str:
        """Notebook file name"""
        return os.path.basename(self.source)

    @model_validator(mode="after")
    def set_paths(self) -> Any:
        # Path set
        if self.path:
            return self

        # root
        if self.rootpath is None:
            self.rootpath = settings.workspace_laktory_root

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        # path
        _path = Path(self.rootpath) / self.dirpath / self.filename
        self.path = _path.as_posix()

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """path with special characters `/`, `.`, `\\` replaced with `-`"""
        key = self.path
        if key is None:
            return ""
        key = key.replace("/", "-")
        key = key.replace("\\", "-")
        key = key.replace(".", "-")
        for i in range(5):
            if key.startswith("-"):
                key = key[1:]
        return key

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
        return ["access_controls", "dirpath", "rootpath"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_notebook"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
