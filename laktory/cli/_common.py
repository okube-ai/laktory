import os
import subprocess
from typing import Any

import yaml
from prompt_toolkit.validation import ValidationError
from prompt_toolkit.validation import Validator
from pydantic import BaseModel

from laktory._logger import get_logger
from laktory.constants import AGENT_CHOICES
from laktory.constants import QUICKSTART_TEMPLATES
from laktory.constants import SUPPORTED_BACKENDS
from laktory.models.stacks.stack import Stack

logger = get_logger(__name__)
DIRPATH = os.path.dirname(__file__)


def parse_cli_vars(
    var_list: list[str],
    var_file: str | None,
    stack_dir: str,
    env: str | None = None,
) -> dict[str, Any]:
    """
    Build a variable dict from --var-file (or auto-discovered variable file)
    followed by --var flags (highest priority).

    Auto-discovery order (when --var-file is not set):
      1. variables.{env}.yaml  (env-specific, e.g. for per-env secrets)
      2. variables.yaml        (base fallback)
    """
    result: dict[str, Any] = {}

    # Resolve var file: explicit path wins, otherwise auto-discover
    resolved_file = var_file
    if resolved_file is None:
        # Try env-specific file first
        if env is not None:
            candidate = os.path.join(stack_dir, f"variables.{env}.yaml")
            if os.path.isfile(candidate):
                resolved_file = candidate
        # Fall back to base file
        if resolved_file is None:
            candidate = os.path.join(stack_dir, "variables.yaml")
            if os.path.isfile(candidate):
                resolved_file = candidate
        if resolved_file is not None:
            logger.info(f"Auto-discovered variable file '{resolved_file}'")

    if resolved_file is not None:
        with open(resolved_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if isinstance(data, dict):
            result.update(data)

    # --var key=value flags override file vars
    for entry in var_list:
        key, _, value = entry.partition("=")
        result[key.strip()] = value

    return result


class TemplateValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in QUICKSTART_TEMPLATES:
            raise ValidationError(
                message=f"Please enter one of the supported template {QUICKSTART_TEMPLATES}",
                cursor_position=len(text),
            )  # Move cursor to end


class AgentValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in AGENT_CHOICES:
            raise ValidationError(
                message=f"Please enter one of the supported agents {AGENT_CHOICES}",
                cursor_position=len(text),
            )


class BackendValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in SUPPORTED_BACKENDS:
            raise ValidationError(
                message=f"Please enter one of the supported IaC backends {SUPPORTED_BACKENDS}",
                cursor_position=len(text),
            )  # Move cursor to end


class CLIController(BaseModel):
    stack_filepath: str | None = None
    env: str | None = None
    auto_approve: bool | None = False
    options_str: str | None = None
    var_list: list[str] = []
    var_file_path: str | None = None
    cli_vars: dict[str, Any] = {}
    stack: Stack | None = None

    def model_post_init(self, __context):
        super().model_post_init(__context)

        # Read Stack Environments
        with open(self.stack_filepath, "r", encoding="utf-8") as fp:
            lines = fp.readlines()

        env_names = []
        envs_found = False
        target_indent = None
        for line in lines:
            if line.strip() == "":
                continue

            if line.strip().startswith("#"):
                continue

            if line.startswith("environments"):
                envs_found = True
                continue

            if envs_found:
                indent = len(line) - len(line.lstrip())
                if target_indent is None:
                    target_indent = indent

                if indent == target_indent:
                    env_names += [line.strip().replace(":", "")]

        # Check environment
        if self.env is None:
            if env_names:
                logger.warning(
                    f"Environment not specified, defaulting to first available ({env_names[0]})"
                )
                self.env = env_names[0]

        logger.info("Stack validation completed.")

        # Resolve CLI vars now that env is known
        stack_dir = os.path.dirname(os.path.abspath(self.stack_filepath))
        self.cli_vars = parse_cli_vars(
            self.var_list, self.var_file_path, stack_dir, self.env
        )

        # Read stack
        if self.stack_filepath is None:
            self.stack_filepath = "./stack.yaml"
        logger.info(f"Reading stack from '{self.stack_filepath}'")
        with open(self.stack_filepath, "r", encoding="utf-8") as fp:
            vars = (
                {"env": self.env, **self.cli_vars}
                if self.env is not None
                else dict(self.cli_vars) or None
            )
            self.stack = Stack.model_validate_yaml(fp, vars=vars or None)

    @property
    def iac_backend(self) -> str:
        return self.stack.iac_backend

    @property
    def organization(self) -> str:
        return self.stack.organization

    @property
    def terraform_options(self):
        options = []
        if self.auto_approve:
            options += ["-auto-approve"]

        if self.options_str:
            options += self.options_str.split(",")

        return options

    def terraform_call(self, cmd):
        pstack = self.stack.to_terraform(env_name=self.env, vars=self.cli_vars or None)
        getattr(pstack, cmd)(flags=self.terraform_options)

    def build(self):
        self.stack.build(env_name=self.env, vars=self.cli_vars or None)


class Worker:
    def run(self, cmd, cwd=None, raise_exceptions=True):
        try:
            subprocess.run(
                cmd,
                cwd=cwd,
                check=True,
            )

        except Exception as e:
            _cmd = " ".join(cmd)
            if raise_exceptions:
                raise e
            else:
                logger.error(f"An error occurred while executing '{_cmd}': {str(e)}")

                # Windows
                c1 = (
                    _cmd.startswith("terraform")
                    and "The system cannot find the file specified".lower()
                    in str(e).lower()
                )

                # Mac/Linux
                c2 = "No such file or directory: 'terraform'".lower() in str(e).lower()

                if c1 or c2:
                    logger.error(
                        "Terraform could not be found. Make sure it is installed and part of the PATH"
                    )
