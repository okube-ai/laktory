from pydantic import Field
from laktory.models.base import BaseModel
from laktory.resources.databricksuser import DatabricksUser


class User(BaseModel):
    user_name: str
    display_name: str = None
    workspace_access: bool = True
    groups: list[str] = []
    roles: list[str] = []
    _resources: DatabricksUser = None

    @property
    def resources(self):
        if self._resources is None:
            self.set_resources()
        return self._resources

    def set_resources(self, name=None, groups=None, **kwargs):
        self._resources = DatabricksUser(model=self, name=name, groups=groups, **kwargs)
        return self._resources
