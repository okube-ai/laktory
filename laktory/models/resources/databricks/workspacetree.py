import os
from pathlib import Path
from pathlib import PurePosixPath

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.virtualterraformresource import VirtualTerraformResource

logger = get_logger(__name__)


class WorkspaceTree(BaseModel, VirtualTerraformResource):
    """
    Databricks Workspace Tree (collections of directories, notebooks and
    workspace files)

    Examples
    --------
    ```py
    import io

    from laktory import models

    tree_yaml = '''
    source: ./source/
    path: /.laktory/source
    '''
    tree = models.resources.databricks.WorkspaceTree.model_validate_yaml(
        io.StringIO(tree_yaml)
    )
    ```

    By default, file content is uploaded verbatim. Opt individual files into
    variable rendering (`${vars.x}` / `${{ expr }}`) via `render_paths` -
    useful for deploying an environment-specific Databricks App source tree,
    for example. Resolved content is staged under `settings.build_root`,
    the original files on disk are never modified.

    ```py
    import io

    from laktory import models

    tree_yaml = '''
    source: ./app/
    path: /apps/myapp
    render_paths:
    - '*.json'
    - configs/*.yaml
    variables:
      env: dev
    '''
    tree = models.resources.databricks.WorkspaceTree.model_validate_yaml(
        io.StringIO(tree_yaml)
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    path: str = Field(
        None,
        description="Workspace filepath for the tree. If not specified, workspace laktory root is used.",
    )
    render_paths: list[str] = Field(
        default=[],
        description=(
            "Glob patterns, relative to `source`, identifying files whose "
            "content is variable-resolved (`${vars.x}` / `${{ expr }}`) and "
            "staged under `settings.build_root` before upload (e.g. "
            "['*.json', 'configs/*.yaml', 'settings/prod.yaml']). Matched "
            "with `PurePosixPath.match()` against each file's path relative "
            "to `source` - a bare pattern like '*.json' matches any file "
            "with that extension at any depth. Nothing is rendered by "
            "default."
        ),
    )
    source: str = Field(
        ...,
        description="Path to directory on local filesystem.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

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
                content = filepath.read_text(encoding="utf-8-sig")
                if "# Databricks notebook source" in content:
                    is_notebook = True
                    language = "PYTHON"
            elif filepath.suffix == ".sql":
                content = filepath.read_text(encoding="utf-8-sig")
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

            # Set variable rendering opt-in
            rel_posix = filepath.relative_to(root).as_posix()
            should_render = any(
                PurePosixPath(rel_posix).match(p) for p in self.render_paths
            )
            kwargs["render_vars"] = should_render
            kwargs["variables"] = self.variables

            if is_notebook:
                r = Notebook(source=str(_source), language=language, **kwargs)
            else:
                r = WorkspaceFile(source=str(_source), **kwargs)

            resources += [r]

        return resources
