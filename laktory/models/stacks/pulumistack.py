import json
import os
from typing import Any
from typing import Union

import yaml
from pydantic import Field

from laktory._logger import get_logger
from laktory._parsers import _resolve_values
from laktory._settings import settings
from laktory._useragent import set_databricks_sdk_upstream
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class ConfigValue(BaseModel):
    type: str = Field("String", description="")
    description: str = Field(None, description="")
    default: Any = Field(None, description="")


class PulumiStack(BaseModel):
    """
    A Pulumi stack is pulumi-specific flavor of the `laktory.models.Stack`. It
    re-structure the attributes to be aligned with a Pulumi.yaml file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_pulumi()`.

    References
    ----------
    * [Stack](https://www.laktory.ai/concepts/stack/)
    * pulumi yaml [options](https://www.pulumi.com/docs/languages-sdks/yaml/yaml-language-reference/)
    """

    name: str = Field(..., description="")
    organization: str = Field(None, exclude=True, description="")
    runtime: str = Field("yaml", description="")
    description: Union[str, None] = Field(None, description="")
    config: dict[str, Union[str, ConfigValue]] = Field({}, description="")
    variables: dict[str, Any] = Field({}, description="")
    resources: dict[str, Any] = Field({}, description="")
    outputs: dict[str, str] = Field({}, description="")

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Pulumi.yaml file."""
        self._configure_serializer(camel=True)
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        for r in self.resources.values():
            d["resources"][r.resource_name] = {
                "type": r.pulumi_resource_type,
                "properties": r.pulumi_properties,
                "options": r.options.model_dump(
                    include=r.options.pulumi_options, exclude_unset=True
                ),
            }

            lookup = r.lookup_existing
            if lookup is not None:
                d["resources"][r.resource_name]["get"] = lookup.pulumi_dump()
                del d["resources"][r.resource_name]["properties"]

        self._configure_serializer(camel=False)

        # Pulumi YAML requires the keyword "resources." to be removed
        _vars = {r"\$\{resources\.(.*?)\}": r"${\1}"}

        # Because all variables are mapped to a string, it is more efficient
        # (>10x) to convert the dict to string before substitution.
        d = json.loads(_resolve_values(json.dumps(d), vars=_vars))

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
        if stack:
            cmd += ["-s", stack]
        if flags:
            cmd += flags

        # Inject user-agent value for monitoring usage as a Databricks partner
        set_databricks_sdk_upstream()

        logger.info(f"Invoking '{' '.join(cmd)}'")
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
