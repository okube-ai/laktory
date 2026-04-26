import os
from pathlib import Path

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.terraformresource import TerraformResource

logger = get_logger(__name__)


class WorkspaceTree(BaseModel, TerraformResource):
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
    resource_name_=None options=ResourceOptions(variables={}, is_enabled=True, depends_on=[], provider=None, ignore_changes=None, import_=None, moved_from=None) lookup_existing=None variables={} access_controls=[] path=None source='./source/'
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
    def additional_core_resources(self) -> list:
        resources = []

        # Get file paths
        source = Path(self.source)
        cwd = Path("./").resolve()
        root = (cwd / source).resolve()
        filepaths = []
        for filepath in root.rglob("*"):
            if filepath.is_dir():
                continue
            if filepath.name.startswith("."):
                continue
            filepaths += [filepath]
        filepaths.sort()

        # Create resources
        for filepath in filepaths:
            # Check if notebook
            is_notebook = filepath.suffix == ".ipynb"
            language = "PYTHON"
            if filepath.suffix == ".py":
                content = filepath.read_text()
                if "# Databricks notebook source" in content:
                    is_notebook = True
                    language = "PYTHON"
            elif filepath.suffix == ".sql":
                content = filepath.read_text()
                if "-- Databricks notebook source" in content:
                    is_notebook = True
                    language = "SQL"

            # Set source (local file system)
            if source.is_absolute():
                _source = str(filepath)
            else:
                _source = Path(os.path.relpath(filepath, cwd))

            # Set path (Databricks / unix file system)
            dirpath = str(filepath.parent).replace(str(root), "")
            if self.path:
                if dirpath.startswith("/"):
                    dirpath = dirpath[1:]
                kwargs = {
                    "path": (Path(self.path) / dirpath / filepath.name).as_posix()
                }
            else:
                kwargs = {"dirpath": Path(dirpath).as_posix()}

            # Set access controls
            kwargs["access_controls"] = self.access_controls

            if is_notebook:
                r = Notebook(source=str(_source), language=language, **kwargs)
            else:
                r = WorkspaceFile(source=str(_source), **kwargs)

            resources += [r]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str | None:
        return None
