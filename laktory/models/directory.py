from typing import Union
from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class Directory(BaseModel, Resources):
    path: str
    delete_recursive: Union[bool, None] = None

    @property
    def key(self):
        key = self.path.replace("/", "-")
        if key.startswith("-"):
            key = key[1:]
        if key.endswith("-"):
            key = key[:-1]
        return key

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def id(self):
        if self._resources is None:
            return None
        return self.resources.directory.id


    @property
    def object_id(self):
        if self._resources is None:
            return None
        return self.resources.directory.object_id

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.directory import PulumiDirectory

        return PulumiDirectory(name=name, directory=self, opts=opts)
