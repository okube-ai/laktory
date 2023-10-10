from typing import Literal
from typing import Union
from typing import Any
from pydantic import BaseModel as _BaseModel

from laktory._settings import settings

# from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
# from laktory.resourcesengines.databricks.base import DatabricksResourcesEngine

ENGINES = ["pulumi", "databricks-api", "terraform"]


class Resources(_BaseModel):
    # _resources: Union[PulumiResourcesEngine, DatabricksResourcesEngine] = None
    _resources: Any = None

    @property
    def resources(self):
        if self._resources is None:
            raise ValueError(f"Model ({self}) has not been deployed. Call model.deploy() first")
        return self._resources

    def deploy(self, *args, engine: Literal[tuple(ENGINES)]=None, **kwargs):
        if not engine:
            engine = settings.resources_engine
        engine = engine.lower()

        if engine == "pulumi":
            self._resources = self.deploy_with_pulumi(*args, **kwargs)
        elif engine == "databricks-api":
            raise NotImplementedError("Databricks API deployments are not yet supported")
            self._resources = self.deploy_with_databricks(*args, **kwargs)
        elif engine == "terraform":
            raise NotImplementedError("Terraform deployments are not yet supported")
            self._resources = self.deploy_with_terraform(*args, **kwargs)
        else:
            raise ValueError(f"Engine {engine} is not supported. Available engines: {ENGINES}")

        return self._resources

    def deploy_with_pulumi(self, *args, **kwargs):
        raise NotImplementedError()

    def deploy_with_databricks(self, *args, **kwargs):
        raise NotImplementedError()

    def deploy_with_terraform(self, *args, **kwargs):
        raise NotImplementedError()

