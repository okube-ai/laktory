import json
import os
import re
from collections import defaultdict
from typing import Any
from typing import Union

from pydantic import model_validator

from laktory._logger import get_logger
from laktory._parsers import _resolve_values
from laktory._settings import settings
from laktory._useragent import set_databricks_sdk_upstream
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel
from laktory.models.resources.providers.baseprovider import BaseProvider

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

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "required_providers": "required_providers",
        }


class TerraformStack(BaseModel):
    """
    A Terraform stack is terraform-specific flavor of the
    `laktory.models.Stack`. It re-structure the attributes to be aligned with a
     terraform json file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_terraform()`.

    References
    ----------
    * [Stack](https://www.laktory.ai/concepts/stack/)
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
                providers[p.resource_name_without_alias] = TerraformRequiredProvider(
                    source=p.source, version=p.version
                )
            self.terraform.required_providers = providers

        return self

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Terraform json file."""
        self._configure_serializer(singular=True)
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        d["resource"] = defaultdict(lambda: {})
        d["data"] = defaultdict(lambda: {})
        for r in self.resources.values():
            _d = r.terraform_properties
            if r.lookup_existing:
                d["data"][r.terraform_resource_lookup_type][r.resource_name] = (
                    r.lookup_existing.model_dump()
                )
                for k in r.options.terraform_options:
                    if k in _d:
                        d["data"][r.terraform_resource_lookup_type][r.resource_name][
                            k
                        ] = _d[k]
            else:
                d["resource"][r.terraform_resource_type][r.resource_name] = _d
        d["data"] = dict(d["data"])
        if len(d["data"]) == 0:
            del d["data"]
        d["resource"] = dict(d["resource"])
        self._configure_serializer(singular=False)

        # Special treatment of moved
        # `moved_from` should generally be used with Terraform, but we also
        # support aliases (Pulumi) for better user experience
        i = -1
        for r in self.resources.values():
            moved_from = r.options.moved_from
            aliases = r.options.aliases
            _from = None
            if moved_from:
                _from = moved_from
            elif aliases:
                _from = aliases[0]
            if _from:
                i += 1
                d[f"moved_{i:05d}"] = {
                    "from": f"{r.terraform_resource_type}.{_from}",
                    "to": f"{r.terraform_resource_type}.{r.resource_name}",
                }

        # Terraform JSON requires the keyword "resources." to be removed and the
        # resource_name to be replaced with resource_type.resource_name.
        _vars = {}
        for r in list(self.resources.values()) + list(self.providers.values()):
            k0 = r.resource_name

            k1 = f"{r.terraform_resource_type}.{r.resource_name}"
            # special treatment for data sources
            if r.lookup_existing:
                k1 = f"data.{r.terraform_resource_lookup_type}.{r.resource_name}"

            if isinstance(r, BaseProvider):
                pattern = r"\$\{resources." + k0 + "}"
                _vars[pattern] = k0

            else:
                # ${resources.resource_name} -> resource_type.resource_name
                pattern = r"\$\{resources\." + k0 + r"\}"
                _vars[pattern] = k1

                # ${resources.resource_name.property} -> ${resource_type.resource_name.property}
                pattern = r"\$\{resources\." + k0 + r"\.(.*?)\}"
                _vars[pattern] = rf"${{{k1}.\1}}"

        # Because all variables are mapped to a string, it is more efficient
        # (>10x) to convert the dict to string before substitution.
        d = json.loads(_resolve_values(json.dumps(d), vars=_vars))

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
        logger.info(f"Writing terraform config at '{filepath}'")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        text = json.dumps(self.model_dump(), indent=4)

        # Terraform stack file is not a strict format. Some keys might be
        # repeated and require special treatment.

        # Special treatment of providers with aliases
        for _, p in self.providers.items():
            if p.alias is not None:
                text = text.replace(
                    f'"{p.resource_name}":',
                    f'"{p.resource_name_without_alias}":',
                )

        # Special treatment of moved
        text = re.sub(r'"moved_\d+": {', '"moved": {', text)

        # Output
        with open(filepath, "w") as fp:
            fp.write(text)

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

        logger.info(f"Invoking '{' '.join(cmd)}'")
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
        Runs `terraform apply`

        Parameters
        ----------
        flags:
            List of flags / options for terraform apply
        """
        self._call("apply", flags=flags)

    def destroy(self, flags: list[str] = None):
        """
        Runs `terraform destroy`

        Parameters
        ----------
        flags:
            List of flags / options for terraform destroy
        """
        self._call("destroy", flags=flags)
