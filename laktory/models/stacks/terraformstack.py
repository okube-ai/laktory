import os
import json
from typing import Any
from typing import Union

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel
from laktory.models.stacks.stack import StackResources

logger = get_logger(__name__)


class ConfigValue(BaseModel):
    type: str = "String"
    description: str = None
    default: Any = None


class TerraformRequiredProvider(BaseModel):
    source: str
    version: str = None


class TerraformConfig(BaseModel):
    required_providers: dict[str, TerraformRequiredProvider] = {
        "databricks": TerraformRequiredProvider(source="databricks/databricks"),

    }


class TerraformStack(BaseModel):
    """
    A Terraform stack is terraform-specific flavor of the
    `laktory.models.Stack`. It re-structure the attributes to be aligned with a
     terraform json file.

    It is generally not instantiated directly, but rather created using
    `laktory.models.Stack.to_terraform()`.

    """
    terraform: TerraformConfig = TerraformConfig()
    provider: dict = {"databricks": {}}
    resource:  StackResources = StackResources()

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        """Serialize model to match the structure of a Terraform json file."""
        kwargs["exclude_none"] = kwargs.get("exclude_none", True)
        d = super().model_dump(*args, **kwargs)

        # Special treatment of resources
        dr0 = {}
        for rtype in self.resource.model_fields:
            dr1 = {}
            resources = getattr(self.resource, rtype)
            for rname, r in resources.items():
                dr1[rname] = r.model_dump(exclude_none=True)

            # Skip resource types without resources
            if dr1:
                dr0[r.terraform_resource_type] = dr1

        d["resource"] = dr0

        d = self.inject_vars(d, target="terraform")

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

        # Need to write providers folder (or to call terraform to create them)

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
