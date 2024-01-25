import os
import json
from collections import defaultdict
from typing import Any
from typing import Union
from pydantic import model_validator
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


class TerraformStack(BaseModel):
    """
    A Terraform stack is terraform-specific flavor of the
    `laktory.models.Stack`. It re-structure the attributes to be aligned with a
     terraform json file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_terraform()`.

    """
    terraform: TerraformConfig = TerraformConfig()
    provider: dict[str, Any] = {}
    resource: dict[str, Any] = {}

    @model_validator(mode="after")
    def required_providers(self) -> Any:
        if self.terraform.required_providers is None:
            providers = {}
            for p in self.provider.values():
                providers[p.resource_name] = TerraformRequiredProvider(source=p.source, version=p.version)
            self.terraform.required_providers = providers

        return self

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Terraform json file."""
        settings.singular_serialization = True
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        d["resource"] = defaultdict(lambda: {})
        for r in self.resource.values():
            _d = r.terraform_properties
            d["resource"][r.terraform_resource_type][r.resource_name] = _d
        d["resource"] = dict(d["resource"])
        settings.singular_serialization = False

        # Pulumi YAML requires the keyword "resources." to be removed
        pattern = "\$\{resources\.(.*?)\}"
        self.variables[pattern] = r"\1"
        d = self.inject_vars(d)
        del self.variables[pattern]

        return d

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def write(self) -> str:
        """
        Write Pulumi.yaml configuration file

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

    def _call(self, command: str, flags: list[str] =None):
        from laktory.cli._worker import Worker

        self.write()
        worker = Worker()

        if not os.path.exists(os.path.join(CACHE_ROOT, ".terraform")):
            worker.run(
                cmd=["terraform", "init"],
                cwd=CACHE_ROOT,
                raise_exceptions=settings.cli_raise_external_exceptions,
            )

        cmd = ["terraform", command]

        if flags is not None:
            cmd += flags

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
            raise_exceptions=settings.cli_raise_external_exceptions,
        )

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


if __name__ == "__main__":
    from laktory import models

    stack = models.Stack(
        name="test",
        resources={
            "directories": {
                "tmp-folder": {
                    "path": "/tmp/"
                }
            }

        }
    )

    tstack = stack.to_terraform()

    import json
    print(json.dumps(tstack.model_dump(), indent=4))

    # tstack.apply(flags=["-auto-approve"])
