import os
from typing import Any
from pydantic import model_validator
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission


class InitScript(BaseModel, Resources):
    source: str
    dirpath: str = "/init_scripts/"
    path: str = None
    permissions: list[Permission] = []

    @property
    def filename(self):
        return os.path.basename(self.source)

    @property
    def key(self):
        key = os.path.splitext(self.path)[0].replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        return key

    @model_validator(mode="after")
    def default_path(self) -> Any:

        if self.path is None:

            if self.dirpath:
                self.path = f"{self.dirpath}{self.filename}"

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "dirpath"]

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.initscript import PulumiInitScript
        return PulumiInitScript(name=name, init_script=self, opts=opts)
