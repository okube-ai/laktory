import os
import json
from collections import defaultdict
from typing import Any
from typing import Union
from pydantic import model_validator

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


class TerraformRequiredProvider(BaseModel):
    source: str
    version: Union[str, None] = None


class TerraformConfig(BaseModel):
    required_providers: dict[str, TerraformRequiredProvider] = None
    backend: Union[dict[str, Any], None] = None


class TerraformStack(BaseModel):
    """
    A Terraform stack is terraform-specific flavor of the
    `laktory.models.Stack`. It re-structure the attributes to be aligned with a
     terraform json file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_terraform()`.

    """

    terraform: TerraformConfig = TerraformConfig()
    providers: dict[str, Any] = {}
    resources: dict[str, Any] = {}

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "providers": "provider",
            "resources": "resource",
        }

    @model_validator(mode="after")
    def required_providers(self) -> Any:
        if self.terraform.required_providers is None:
            providers = {}
            for p in self.providers.values():
                providers[p.resource_name] = TerraformRequiredProvider(
                    source=p.source, version=p.version
                )
            self.terraform.required_providers = providers

        return self

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Terraform json file."""
        settings.singular_serialization = True
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        d["resource"] = defaultdict(lambda: {})
        for r in self.resources.values():
            _d = r.terraform_properties
            d["resource"][r.terraform_resource_type][r.resource_name] = _d
        d["resource"] = dict(d["resource"])
        settings.singular_serialization = False

        # Pulumi YAML requires the keyword "resources." to be removed and the
        # resource_name to be replaced with resource_type.resource_name.
        patterns = []
        for r in list(self.resources.values()) + list(self.providers.values()):
            k0 = r.resource_name
            k1 = f"{r.terraform_resource_type}.{r.resource_name}"
            # special treatment for resources without type (providers)
            if r.terraform_resource_type is None:
                k1 = k0

            # ${resources.resource_name} -> resource_type.resource_name
            pattern = r"\$\{resources\." + k0 + r"\}"
            self.variables[pattern] = k1
            patterns += [pattern]

            # ${resources.resource_name.property} -> ${resource_type.resource_name.property}
            pattern = r"\$\{resources\." + k0 + r"\.(.*?)\}"
            self.variables[pattern] = rf"${{{k1}.\1}}"
            patterns += [pattern]

        d = self.inject_vars(d)
        for p in patterns:
            del self.variables[p]

        return d

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def write(self) -> str:
        """
        Write Terraform json configuration file

        Returns
        -------
        :
            Filepath of the configuration file
        """
        filepath = os.path.join(CACHE_ROOT, "stack.tf.json")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            json.dump(self.model_dump(), fp, indent=4)

        return filepath

    def _call(self, command: str, flags: list[str] = None):
        from laktory.cli._common import Worker

        self.write()
        worker = Worker()

        cmd = ["terraform", command]

        if flags is not None:
            cmd += flags

        # Inject user-agent value for monitoring usage as a Databricks partner
        set_databricks_sdk_upstream()

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
            raise_exceptions=settings.cli_raise_external_exceptions,
        )

    def init(self, flags: list[str] = None) -> None:
        """
        Runs `terraform init`

        Parameters
        ----------
        flags:
            List of flags / options for pulumi plan
        """
        self._call("init", flags=flags)

    def plan(self, flags: list[str] = None) -> None:
        """
        Runs `terraform plan`

        Parameters
        ----------
        flags:
            List of flags / options for pulumi plan
        """
        self._call("plan", flags=flags)

    def apply(self, flags: list[str] = None):
        """
        Runs `pulumi apply`

        Parameters
        ----------
        flags:
            List of flags / options for terraform apply
        """
        self._call("apply", flags=flags)
