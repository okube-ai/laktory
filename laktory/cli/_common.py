import os
import subprocess
from pydantic import BaseModel
from typing import Union
from prompt_toolkit.validation import Validator
from prompt_toolkit.validation import ValidationError

from laktory.models.stacks.stack import Stack
from laktory.constants import SUPPORTED_BACKENDS

DIRPATH = os.path.dirname(__file__)


class BackendValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in SUPPORTED_BACKENDS:
            raise ValidationError(
                message=f"Please enter one of the supported IaC backends {SUPPORTED_BACKENDS}",
                cursor_position=len(text),
            )  # Move cursor to end


class CLIController(BaseModel):
    stack_filepath: Union[str, None] = None
    backend: Union[str, None] = None
    organization: Union[str, None] = None
    env: Union[str, None] = None
    pulumi_options_str: Union[str, None] = None
    terraform_options_str: Union[str, None] = None
    stack: Union[Stack, None] = None

    def model_post_init(self, __context):
        super().model_post_init(__context)

        # Read stack
        if self.stack_filepath is None:
            self.stack_filepath = "./stack.yaml"
        with open(self.stack_filepath, "r") as fp:
            self.stack = Stack.model_validate_yaml(fp)

        # Set backend
        if self.backend is None:
            self.backend = self.stack.backend
        if self.backend is None:
            raise ValueError(
                "backend ['pulumi', 'terraform'] must be specified in stack file or as CLI option "
            )

        # Set organization
        if self.organization is None:
            self.organization = self.stack.organization
        if self.organization is None and self.backend == "pulumi":
            raise ValueError("organization must be specified with pulumi backend")

        # Check environment
        if self.env is None and self.backend == "pulumi":
            raise ValueError("Environment must be specified with pulumi backend")

    @property
    def pulumi_options(self):
        if self.pulumi_options_str is None:
            return None
        return self.pulumi_options_str.split(",")

    @property
    def terraform_options(self):
        if self.terraform_options_str is None:
            return None
        return self.terraform_options_str.split(",")

    @property
    def pulumi_stack_name(self):
        return self.organization + "/" + self.env

    def pulumi_call(self, cmd):
        if self.pulumi_stack_name is None:
            raise ValueError("Argument `stack` must be specified with pulumi backend")

        pstack = self.stack.to_pulumi(env=self.env)
        getattr(pstack, cmd)(stack=self.pulumi_stack_name, flags=self.pulumi_options)

    def terraform_call(self, cmd):
        pstack = self.stack.to_terraform(env=self.env)
        getattr(pstack, cmd)(flags=self.terraform_options)


class Worker:
    def run(self, cmd, cwd=None, raise_exceptions=True):
        try:
            completed_process = subprocess.run(
                cmd,
                cwd=cwd,
                check=True,
            )

        except Exception as e:
            if raise_exceptions:
                raise e
            else:
                print("An error occurred:", str(e))
