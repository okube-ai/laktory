import json
from typing import Any
from typing import Union

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import camelize_keys
from laktory.models.stacks.basestack import BaseStack
from laktory._logger import get_logger

logger = get_logger(__name__)


class ConfigValue(BaseModel):
    type: str = "String"
    description: str = None
    default: Any = None


class PulumiStack(BaseStack):
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

    def model_dump(self, *args, keys_to_camel_case=True, **kwargs) -> dict[str, Any]:
        """TODO"""
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        # kwargs["keys_to_camel_case"] = False
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        for r in self.resources.values():
            d["resources"][r.resource_name] = {
                "type": r.pulumi_resource_type,
                "properties": r.pulumi_properties
            }
        d["resources"] = self.resolve_vars(d["resources"], target="pulumi")

        if keys_to_camel_case:
            d = camelize_keys(d)

        return d

    def model_dump_json(self, *args, **kwargs) -> str:
        """TODO"""
        d = self.model_dump(*args, **kwargs)
        return json.dumps(d, indent=4)