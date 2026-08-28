import os
from pathlib import Path
from pathlib import PurePosixPath

from pathspec import PathSpec
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

    Files can be excluded from the tree with `exclude_paths`, using
    `.gitignore` syntax (including negation) - independent of git, so a file
    can be committed to the repo and still be kept out of the deployed tree.
    A real `.gitignore` found at the root of `source` can also be honored via
    `use_gitignore`, for the common case where "don't commit" and "don't
    deploy" coincide. Dotfiles and dot-directories (e.g. `.git`, `.venv`) are
    excluded by default; re-include one explicitly with a negated pattern
    (e.g. `!.streamlit/`, `!.streamlit/**`) - useful for a Databricks App
    source tree that relies on a dotdir like `.streamlit/config.toml`.

    ```py
    import io

    from laktory import models

    tree_yaml = '''
    source: ./app/
    path: /apps/myapp
    exclude_paths:
    - '*.log'
    - build/
    - '!.streamlit/'
    - '!.streamlit/**'
    use_gitignore: true
    '''
    tree = models.resources.databricks.WorkspaceTree.model_validate_yaml(
        io.StringIO(tree_yaml)
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    exclude_paths: list[str] = Field(
        default=[],
        description=(
            "Gitignore-style patterns, relative to `source`, identifying "
            "files and directories to exclude from the tree (e.g. "
            "['*.log', 'build/', '!build/keep.txt']). Matched with the same "
            "syntax as a `.gitignore` file (supports negation). Applied "
            "after the built-in dotfile/dot-directory exclusion and, when "
            "`use_gitignore` is enabled, after `source/.gitignore` - so it "
            "wins on conflicting/negated patterns, and can be used to "
            "re-include a dotdir (e.g. '!.streamlit/', '!.streamlit/**')."
        ),
    )
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
    use_gitignore: bool = Field(
        default=False,
        description=(
            "If `True` and a `.gitignore` file exists at the root of "
            "`source`, its patterns are automatically applied when building "
            "the tree. Off by default, since it changes which files deploy "
            "based on a file unrelated to this resource. Governs git "
            "tracking, not deployment - use `exclude_paths` for files that "
            "are committed but shouldn't be deployed."
        ),
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

        # Build ignore spec. Dotfiles/dot-directories are excluded by
        # default (backward compatible), but - unlike a hardcoded check -
        # this is just the first pattern in the spec, so a later negated
        # pattern in exclude_paths can re-include one (e.g. a Databricks App
        # relying on `.streamlit/config.toml`).
        patterns = [".*"]
        if self.use_gitignore:
            gitignore_path = root / ".gitignore"
            if gitignore_path.is_file():
                patterns += gitignore_path.read_text(encoding="utf-8-sig").splitlines()
        patterns += self.exclude_paths
        spec = PathSpec.from_lines("gitignore", patterns)

        filepaths = []
        for filepath in root.rglob("*"):
            if filepath.is_dir():
                continue
            rel_path = filepath.relative_to(root)
            if spec.match_file(rel_path.as_posix()):
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

            # Set path (Databricks / unix file system). Computed via
            # `relative_to().as_posix()` instead of stringifying `filepath.parent`
            # and stripping `root` - the latter uses the OS-native separator
            # (`\` on Windows), which `Path(...) / dirpath` then treats as an
            # absolute anchor and silently discards any preceding path segment.
            dirpath = filepath.relative_to(root).parent.as_posix()
            if self.path:
                kwargs = {
                    "path": (Path(self.path) / dirpath / filepath.name).as_posix()
                }
            else:
                kwargs = {"dirpath": dirpath}

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
