import os
from typing import Any
from typing import Literal
from pydantic import model_validator
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission


class Notebook(BaseModel, Resources):
    source: str
    dirpath: str = None
    path: str = None
    language: Literal["SCALA", "PYTHON", "SQL", "R"] = None
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

            elif "/notebooks/" in self.source:
                self.path = "/" + self.source.split("/notebooks/")[-1]

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.notebook import PulumiNotebook
        return PulumiNotebook(name=name, notebook=self, opts=opts)
