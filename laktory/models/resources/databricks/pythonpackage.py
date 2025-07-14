import os
from pathlib import Path
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory import settings
from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

logger = get_logger(__name__)


class PythonPackage(BaseModel, PulumiResource, TerraformResource):
    """
    Python Package built and deployed as a wheel file.

    This resource type allows to target a local python package to generate a wheel file
    that will be built at deploy and deployed as a WorkspaceFile.

    Examples
    --------
    ```py
    import laktory as lk

    pp = lk.models.resources.databricks.PythonPackage(
        package_name="lake",
        config_filepath="lake/pyproject.toml",
        dirpath="/wheels/",
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    build_command: str = Field(
        "uv build --wheel",
        description="The build command used to generate the wheel file.",
    )
    config_filepath: str = Field(
        ...,
        description="File path of the pyproject.toml file or setup.py configuration file.",
    )
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the workspace file is deployed. Used only if `path` is not specified.",
    )
    package_name: str = Field(..., description="Name of the package")
    path_: str = Field(
        None,
        description="Workspace filepath for the file. Overwrite `rootpath` and `dirpath`.",
        validation_alias=AliasChoices("path_", "path"),
        exclude=True,
    )
    rootpath: str = Field(
        None,
        description="""
    Root directory to which all workspace files are deployed to. Can also be configured by settings 
    LAKTORY_WORKSPACE_LAKTORY_ROOT environment variable. Default is `/.laktory/`. Used only if `path` is not specified.
    """,
    )
    # wheel_filename: str = Field(None, description="Overrides default wheel filename")
    _wheel_path: Path = None

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {"path": ""}

    @property
    def filename(self) -> str | None:
        """File filename"""
        if self.source:
            return os.path.basename(self.source)

    @computed_field(description="path")
    @property
    def path(self) -> str | None:
        # Path set
        if self.path_:
            return self.path_

        if not self.source:
            return None

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

        return _path.as_posix()

    @computed_field(description="Wheel file path", return_type=str)
    @property
    def source(self) -> str:
        self.build_package()
        return str(self._wheel_path)

    def build_package(self):
        # Ideally we would want to deterministically find the value of wheel_path
        # without building it, but it seems cumbersome and unreliable to parse
        # the config file and find the version.
        if self._wheel_path is None:
            from laktory.cli._common import Worker

            if not Path(self.config_filepath).exists():
                raise ValueError(
                    f"Config filepath {Path(self.config_filepath).absolute().resolve()} ({self.config_filepath}) for package '{self.package_name}' doest not exists"
                )

            package_root = Path(self.config_filepath).parent
            dist_path = package_root / "dist"

            worker = Worker()

            cmd = self.build_command.split(" ")

            logger.info(
                f"Building package '{self.package_name}' from {package_root} with '{' '.join(cmd)}'"
            )
            worker.run(
                cmd=cmd,
                cwd=package_root,
                raise_exceptions=settings.cli_raise_external_exceptions,
            )

            files = [f for f in dist_path.glob("*.whl") if f.is_file()]
            if len(files) == 0:
                raise RuntimeError(
                    "Wheel file could not be built. Make sure the `config_filepath` and `build_command` are correct."
                )
            file = max(files, key=lambda f: f.stat().st_mtime)

            self._wheel_path = file

            # Renaming wheel file
            # if self.wheel_filename:
            #     new_path = self._wheel_path.with_name(self.wheel_filename)
            #     logger.info(f"Renaming {self._wheel_path} -> {new_path}")
            #     self._wheel_path.rename(new_path)
            #     self._wheel_path = new_path

            self._wheel_path = self._wheel_path.absolute().resolve()

        return self._wheel_path

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """package name"""
        return self.package_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    workspace_file_path=self.path,
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:WorkspaceFile"

    @property
    def pulumi_excludes(self) -> list[str] | dict[str, bool]:
        return [
            "access_controls",
            "build_command",
            "config_filepath",
            "dirpath",
            "package_name",
            "rootpath",
            "wheel_filename",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_file"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
