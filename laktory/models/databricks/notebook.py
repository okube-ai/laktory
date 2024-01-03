import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.permission import Permission
from laktory.models.databricks.permissions import Permissions


class Notebook(BaseModel, PulumiResource):
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
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Notebook resource key"""
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

            res += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.permissions,
                    notebook_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return res

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Notebook"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks
        return databricks.Notebook

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["permissions", "dirpath"]
