from pydantic import Field
from laktory.models.base import BaseModel
from laktory.resources.databricksserviceprincipal import DatabricksServicePrincipal


class ServicePrincipal(BaseModel):
    display_name: str
    application_id: str = None
    allow_cluster_create: bool = False
    groups: list[str] = []
    roles: list[str] = []
    _resources: DatabricksServicePrincipal = None

    @property
    def resources(self):
        if self._resources is None:
            self.set_resources()
        return self._resources

    def set_resources(self, name=None, groups=None, **kwargs):
        self._resources = DatabricksServicePrincipal(model=self, name=name, groups=groups, **kwargs)
        return self._resources
