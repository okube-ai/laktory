from laktory.models.base import BaseModel
from laktory.resources.databricksgroup import DatabricksGroup


class Group(BaseModel):
    display_name: str
    allow_cluster_create: bool = False
    workspace_access: bool = True
    user_names: list[str] = None
    _resources: DatabricksGroup = None

    @property
    def resources(self):
        if self._resources is None:
            self.set_resources()
        return self._resources

    def set_resources(self, name=None, **kwargs):
        self._resources = DatabricksGroup(model=self, name=name, **kwargs)
        return self._resources
