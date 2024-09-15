import os
import subprocess
from pydantic import BaseModel
from typing import Union
from prompt_toolkit.validation import Validator
from prompt_toolkit.validation import ValidationError

from laktory.models.stacks.stack import Stack
from laktory.constants import SUPPORTED_BACKENDS
from laktory.constants import QUICKSTART_TEMPLATES
from laktory._logger import get_logger

logger = get_logger(__name__)
DIRPATH = os.path.dirname(__file__)


class TemplateValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in QUICKSTART_TEMPLATES:
            raise ValidationError(
                message=f"Please enter one of the supported template {QUICKSTART_TEMPLATES}",
                cursor_position=len(text),
            )  # Move cursor to end


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
    env: Union[str, None] = None
    auto_approve: Union[bool, None] = False
    options_str: Union[str, None] = None
    stack: Union[Stack, None] = None

    def model_post_init(self, __context):
        super().model_post_init(__context)

        # Read stack
        if self.stack_filepath is None:
            self.stack_filepath = "./stack.yaml"
        with open(self.stack_filepath, "r", encoding="utf-8") as fp:
            self.stack = Stack.model_validate_yaml(fp)

        # Check environment
        if self.env is None:
            env_names = list(self.stack.environments.keys())
            if env_names:
                logger.warn(
                    f"Environment not specified, defaulting to first available ({env_names[0]})"
                )
                self.env = env_names[0]

    @property
    def backend(self) -> str:
        return self.stack.backend

    @property
    def organization(self) -> str:
        return self.stack.organization

    @property
    def pulumi_options(self):
        options = []
        if self.auto_approve:
            options += ["--yes"]

        if self.options_str:
            options += self.options_str.split(",")

        return options

    @property
    def terraform_options(self):
        options = []
        if self.auto_approve:
            options += ["-auto-approve"]

        if self.options_str:
            options += self.options_str.split(",")

        return options

    @property
    def pulumi_stack_name(self):
        return self.organization + "/" + self.env

    def pulumi_call(self, cmd):
        if self.pulumi_stack_name is None:
            raise ValueError("Argument `stack` must be specified with pulumi backend")

        pstack = self.stack.to_pulumi(env_name=self.env)
        getattr(pstack, cmd)(stack=self.pulumi_stack_name, flags=self.pulumi_options)

    def terraform_call(self, cmd):
        pstack = self.stack.to_terraform(env_name=self.env)
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
            _cmd = " ".join(cmd)
            if raise_exceptions:
                raise e
            else:
                print(f"An error occurred while executing '{_cmd}': {str(e)}")

                # Windows
                c1 = (
                    _cmd.startswith("terraform")
                    and "The system cannot find the file specified".lower()
                    in str(e).lower()
                )
                c2 = (
                    _cmd.startswith("pulumi")
                    and "The system cannot find the file specified".lower()
                    in str(e).lower()
                )

                # Mac/Linux
                c3 = "No such file or directory: 'terraform'".lower() in str(e).lower()
                c4 = "No such file or directory: 'pulumi'".lower() in str(e).lower()

                if c1 or c3:
                    print(
                        "Terraform is selected as IaC backend. Make sure it is installed and part of the PATH"
                    )
                elif c2 or c4:
                    print(
                        "Pulumi is selected as IaC backend. Make sure it is installed and part of the PATH"
                    )
