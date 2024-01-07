import json
from typing import Any
from typing import Union

from laktory._parsers import camelize_keys
from laktory.models.basemodel import BaseModel
from laktory._logger import get_logger

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
