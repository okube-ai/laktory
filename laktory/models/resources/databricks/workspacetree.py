from pathlib import Path

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceTree(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Workspace Tree (collections of directories, notebooks and
    workspace files)

    Examples
    --------
    ```py
    from laktory import models

    tree = models.resources.databricks.WorkspaceTree(
        source="./source/",
    )
    print(tree)
    '''
    resource_name_=None options=ResourceOptions(variables={}, is_enabled=True, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None, moved_from=None) lookup_existing=None variables={} access_controls=[] path=None source='./source/'
    '''
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    path: str = Field(
        None,
        description="Workspace filepath for the tree. If not specified, workspace laktory root is used.",
    )
    source: str = Field(
        ...,
        description="Path to directory on local filesystem.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def self_as_core_resources(self) -> bool:
        return False

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        resources = []

        root = Path(self.source).resolve()

        for filepath in root.rglob("*"):
            if filepath.is_dir():
                continue

            # Check if notebook
            is_notebook = filepath.suffix == ".ipynb"
            if filepath.suffix == ".py":
                content = filepath.read_text()
                if "# Databricks notebook source" in content:
                    is_notebook = True

            # Set path
            dirpath = str(filepath.parent).replace(str(root), "")
            if self.path:
                if dirpath.startswith("/"):
                    dirpath = dirpath[1:]
                kwargs = {
                    "path": (Path(self.path) / dirpath / filepath.name).as_posix()
                }
            else:
                kwargs = {"dirpath": dirpath}

            # Set access controls
            kwargs["access_controls"] = self.access_controls

            if is_notebook:
                r = Notebook(source=str(filepath), **kwargs)
            else:
                r = WorkspaceFile(source=str(filepath), **kwargs)

            resources += [r]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str | None:
        return None

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str | None:
        return None
