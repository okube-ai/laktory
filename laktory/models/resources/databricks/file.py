import os
from pathlib import PurePosixPath

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory.models.resources.databricks._renderablefile import RenderableFileMixin
from laktory.models.resources.databricks.file_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.file_base import FileBase


class File(RenderableFileMixin, FileBase):
    """
    Databricks Unity Catalog Volume File

    File uploaded to a Unity Catalog volume. Files can also be declared
    directly in the `files` of a `Volume`, with `dirpath` relative to the
    volume root.

    Examples
    --------
    ```py
    import io

    from laktory import models

    file_yaml = '''
    source: ./data/stock_prices/prices.json
    dirpath: /Volumes/dev/sources/landing/stock_prices/
    '''
    file = models.resources.databricks.File.model_validate_yaml(io.StringIO(file_yaml))
    print(file.path)
    # > /Volumes/dev/sources/landing/stock_prices/prices.json
    ```

    References
    ----------

    * [Databricks Volumes](https://docs.databricks.com/aws/en/volumes/)
    * [Terraform databricks_file](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/file)
    """

    dirpath: str = Field(
        None,
        description="""
    Volume directory in which the file is deployed, e.g. `/Volumes/dev/sources/landing/configs/`. When declared in the
    `files` of a volume, relative to the volume root. Used only if `path` is not specified.
    """,
    )
    path_: str = Field(
        None,
        description="Volume filepath for the file, e.g. `/Volumes/dev/sources/landing/file.txt`. Overwrite `dirpath`.",
        validation_alias=AliasChoices("path", "path_"),
        exclude=True,
    )
    source_: str = Field(
        None,
        description="The full absolute path to the file. Conflicts with `content_base64`",
        validation_alias=AliasChoices("source", "source_"),
        exclude=True,
    )

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        if self.path_:
            return self.path_
        if self.dirpath is None or self.source_ is None:
            return None
        return (PurePosixPath(self.dirpath) / self.filename).as_posix()

    @computed_field(description="source")
    @property
    def source(self) -> str | None:
        # `path` is unset until a nested file gets its volume `dirpath`
        if self.render_vars and self.path:
            return self._staged_path("files", self.path)
        return self.source_

    @property
    def filename(self) -> str:
        """File filename"""
        return os.path.basename(self.source_)

    def build(self, vars: dict = None):
        """
        Render `${vars.x}`/`${{ expr }}` placeholders in the file content
        and stage the result under `settings.build_root`, if `render_vars`
        is `True`. No-op otherwise.
        """
        if self.render_vars:
            self._render_to_staged_path(self.source_, self.source, vars=vars)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.path

    @property
    def _resource_safe_key_has_type_id(self) -> bool:
        # Paths often contain "file" (e.g. `profiles.json`), so only an explicit
        # prefix counts
        return self.resource_safe_key.startswith(f"{self.resource_type_id}-")

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["dirpath", "render_vars"]
