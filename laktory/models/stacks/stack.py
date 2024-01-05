import os
import yaml
from typing import Any

from laktory._logger import get_logger
from laktory._worker import Worker
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel
from laktory.models.stacks.pulumistack import PulumiStack

logger = get_logger(__name__)

DIRPATH = "./"


class StackEnvironment(BaseModel):
    pass


class StackVariable(BaseModel):
    pass


class Stack(BaseModel):
    """
    The Stack defines a group of deployable resources.
    """

    name: str
    config: dict[str, str] = None
    description: str = None
    resources: list[Any]
    environments: list[StackEnvironment] = []
    variables: dict[str, str] = {}
    pulumi_outputs: dict[str, str] = {}  # TODO

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    def to_pulumi_stack(self):
        resources = {}

        for r in self.resources:
            for _r in r.resources:
                resources[_r.resource_name] = _r

        return PulumiStack(
            name=self.name,
            config=self.config,
            description=self.description,
            resources=resources,
            # variables=None,  # TODO
            outputs=self.pulumi_outputs,
        )

    def write_pulumi_stack(self) -> str:
        # TODO: Write environment configs
        filepath = os.path.join(CACHE_ROOT, "Pulumi.yaml")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            yaml.dump(self.to_pulumi_stack().model_dump(), fp)

        return filepath

    def _pulumi_call(self, command, stack=None, flags=None):
        filepath = self.write_pulumi_stack()
        worker = Worker()

        cmd = ["pulumi", command]

        # Stack
        if stack is not None:
            cmd += ["-s", stack]

        if flags is not None:
            cmd += flags

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
        )

    def pulumi_preview(self, stack=None, flags=None):
        self._pulumi_call("preview", stack=stack, flags=flags)

    def pulumi_up(self, stack=None, flags=None):
        self._pulumi_call("up", stack=stack, flags=flags)

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def model_terraform_dump(self):
        pass
