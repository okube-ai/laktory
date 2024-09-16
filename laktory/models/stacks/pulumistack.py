import os
import yaml
from typing import Any
from typing import Union
from pydantic import Field

from laktory._useragent import set_databricks_sdk_upstream
from laktory._logger import get_logger
from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class ConfigValue(BaseModel):
    type: str = "String"
    description: str = None
    default: Any = None


class PulumiStack(BaseModel):
    """
    A Pulumi stack is pulumi-specific flavor of the `laktory.models.Stack`. It
    re-structure the attributes to be aligned with a Pulumi.yaml file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_pulumi()`.

    References
    ----------
    - pulumi yaml [options](https://www.pulumi.com/docs/languages-sdks/yaml/yaml-language-reference/)
    """

    name: str
    organization: str = Field(None, exclude=True)
    runtime: str = "yaml"
    description: Union[str, None] = None
    config: dict[str, Union[str, ConfigValue]] = {}
    variables: dict[str, Any] = {}
    resources: dict[str, Any] = {}
    outputs: dict[str, str] = {}

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Pulumi.yaml file."""
        settings.camel_serialization = True
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        for r in self.resources.values():
            d["resources"][r.resource_name] = {
                "type": r.pulumi_resource_type,
                "properties": r.pulumi_properties,
                "options": r.options.model_dump(exclude_none=True),
            }

            lookup = r.lookup_existing
            if lookup is not None:
                d["resources"][r.resource_name]["get"] = lookup.pulumi_dump()
                del d["resources"][r.resource_name]["properties"]

        settings.camel_serialization = False

        # Pulumi YAML requires the keyword "resources." to be removed
        pattern = r"\$\{resources\.(.*?)\}"
        self.variables[pattern] = r"${\1}"
        d = self.inject_vars(d)
        del self.variables[pattern]

        return d

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    def write(self) -> str:
        """
        Write Pulumi.yaml configuration file

        Returns
        -------
        :
            Filepath of the configuration file
        """
        filepath = os.path.join(CACHE_ROOT, "Pulumi.yaml")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            yaml.dump(self.model_dump(), fp)

        return filepath

    def _call(self, command: str, stack: str, flags: list[str] = None):
        from laktory.cli._common import Worker

        self.write()
        worker = Worker()

        cmd = ["pulumi", command]
        cmd += ["-s", stack]

        if flags is not None:
            cmd += flags

        # Inject user-agent value for monitoring usage as a Databricks partner
        set_databricks_sdk_upstream()

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
            raise_exceptions=settings.cli_raise_external_exceptions,
        )

    def preview(self, stack: str = None, flags: list[str] = None) -> None:
        """
        Runs `pulumi preview`

        Parameters
        ----------
        stack:
            Name of the stack to use
        flags:
            List of flags / options for pulumi preview
        """
        self._call("preview", stack=stack, flags=flags)

    def up(self, stack: str = None, flags: list[str] = None):
        """
        Runs `pulumi up`

        Parameters
        ----------
        stack:
            Name of the stack to use
        flags:
            List of flags / options for pulumi up
        """
        self._call("up", stack=stack, flags=flags)

    def destroy(self, stack: str = None, flags: list[str] = None):
        """
        Runs `pulumi destroy`

        Parameters
        ----------
        stack:
            Name of the stack to use
        flags:
            List of flags / options for pulumi up
        """
        self._call("destroy", stack=stack, flags=flags)
