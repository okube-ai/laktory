from typing import Literal
from typing import Union
from typing import Any
import re
from pydantic import BaseModel as _BaseModel

from laktory._settings import settings

# from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
# from laktory.resourcesengines.databricks.base import DatabricksResourcesEngine

ENGINES = ["pulumi", "databricks-api", "terraform"]


class BaseResource(_BaseModel):
    """
    Parent class for all Laktory models deployable as one or multiple cloud
    resources. This `BaseResource` class is derived from `pydantic.BaseModel`.
    """

    _resources: Any = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def resources(self) -> list:
        """List of deployed resources"""
        if self._resources is None:
            raise ValueError(
                f"Model ({self}) has not been deployed. Call model.deploy() first"
            )
        return self._resources

    @property
    def pulumi_excludes(self) -> list[str]:
        """List of fields to exclude when dumping model to pulumi"""
        return []

    @property
    def pulumi_renames(self) -> dict[str, str]:
        """Map of fields to rename when dumping model to pulumi"""
        return {}

    @property
    def resource_type_id(self) -> str:
        """Resource type id used to build resource name"""
        _id = type(self).__name__
        _id = re.sub(
            r"(?<!^)(?=[A-Z])", "-", _id
        ).lower()  # Convert CamelCase to kebab-case
        return _id

    @property
    def resource_key(self) -> str:
        """Resource key used to build resource name"""
        return self.name

    @property
    def resource_name(self) -> str:
        """Resource name `{self.resource_type}.{self.resource_key}`"""
        return f"{self.resource_type_id}-{self.resource_key}"

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def model_pulumi_dump(self, *args, **kwargs) -> dict:
        """
        Dump model and customize it to be used as an input for a pulumi
        resource:

        * dump the model
        * remove excludes defined in `self.pulumi_excludes`
        * rename keys according to `self.pulumi_renames`
        * inject variables

        Returns
        -------
        :
            Pulumi-safe model dump
        """
        kwargs["exclude"] = self.pulumi_excludes
        d = super().model_dump(*args, **kwargs)
        for k, v in self.pulumi_renames.items():
            d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d

    def deploy(self, *args, engine: Literal[tuple(ENGINES)] = None, **kwargs) -> list:
        """
        Deploy model resources provided a deployment `engine`.

        Parameters
        ----------
        args:
            Arguments passed to deployment engine
        engine:
            Selected deployment engine. Default value: `settings.resources_engine`
        kwargs
            Keyword arguments to deployment engine

        Returns
        -------
        :
            List of deployed resources
        """
        if not engine:
            engine = settings.resources_engine
        engine = engine.lower()

        if engine == "pulumi":
            self._resources = self.deploy_with_pulumi(*args, **kwargs)
        elif engine == "databricks-api":
            raise NotImplementedError(
                "Databricks API deployments are not yet supported"
            )
            self._resources = self.deploy_with_databricks(*args, **kwargs)
        elif engine == "terraform":
            raise NotImplementedError("Terraform deployments are not yet supported")
            self._resources = self.deploy_with_terraform(*args, **kwargs)
        else:
            raise ValueError(
                f"Engine {engine} is not supported. Available engines: {ENGINES}"
            )

        return self._resources

    def deploy_with_pulumi(self, *args, **kwargs):
        raise NotImplementedError()

    def deploy_with_databricks(self, *args, **kwargs):
        raise NotImplementedError()

    def deploy_with_terraform(self, *args, **kwargs):
        raise NotImplementedError()
