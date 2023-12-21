from typing import Any
from typing import Union

from laktory.models.basemodel import BaseModel
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

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)
        d["resources"] = self.resolve_vars(d["resources"], target="pulumi")
        return d
