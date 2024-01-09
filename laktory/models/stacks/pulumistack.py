import os
import yaml
from typing import Any
from typing import Union

from laktory._logger import get_logger
from laktory._parsers import camelize_keys
from laktory._worker import Worker
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class ConfigValue(BaseModel):
    type: str = "String"
    description: str = None
    default: Any = None


class PulumiStack(BaseModel):
    """
    A stack, as defined by pulumi for deployment.
    """

    name: str
    runtime: str = "yaml"
    description: Union[str, None] = None
    config: dict[str, Union[str, ConfigValue]] = {}
    variables: dict[str, Any] = {}
    resources: dict[str, Any] = {}
    outputs: dict[str, str] = {}

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """TODO"""
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        for r in self.resources.values():
            d["resources"][r.resource_name] = {
                "type": camelize_keys(r.pulumi_resource_type),
                "properties": camelize_keys(r.pulumi_properties),
                "options": camelize_keys(r.options.model_dump(exclude_none=True)),
            }

        d = self.inject_vars(d, target="pulumi_yaml")

        return d

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    def write(self) -> str:
        filepath = os.path.join(CACHE_ROOT, "Pulumi.yaml")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            yaml.dump(self.model_dump(), fp)

        return filepath

    def _call(self, command, stack, flags=None):
        self.write()
        worker = Worker()

        cmd = ["pulumi", command]
        cmd += ["-s", stack]

        if flags is not None:
            cmd += flags

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
        )

    def preview(self, stack=None, flags=None):
        self._call("preview", stack=stack, flags=flags)

    def up(self, stack=None, flags=None):
        self._call("up", stack=stack, flags=flags)
