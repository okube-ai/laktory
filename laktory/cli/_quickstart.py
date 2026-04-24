import os
import shutil
from typing import Annotated

import typer
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

from laktory._version import VERSION
from laktory.cli._common import TemplateValidator
from laktory.cli.app import app
from laktory.constants import QUICKSTART_TEMPLATES


@app.command()
def quickstart(
    template: Annotated[
        str,
        typer.Option(
            "--template",
            "-t",
            help="Template [unity-catalog, workspace, workflows, local-pipeline]",
        ),
    ] = None,
):
    """
    Build get started stack in the calling directory.

    Parameters
    ----------
    template:
        Stack template [unity-catalog, workspace, workflows]

    Examples
    --------
    ```cmd
    laktory quickstart
    ```

    References
    ----------
    * [CLI](https://www.laktory.ai/concepts/cli/)
    """

    # Template
    completer = WordCompleter(QUICKSTART_TEMPLATES, ignore_case=True)
    if template is None:
        template = prompt(
            f"Select template {QUICKSTART_TEMPLATES}: ",
            completer=completer,
            validator=TemplateValidator(),
        )

    # Copy template
    stacks_dir = os.path.join(
        os.path.dirname(__file__), "../resources/quickstart-stacks/"
    )
    source_dir = os.path.join(stacks_dir, template)
    target_dir = "./"

    # Iterate through files
    for root, dits, filenames in os.walk(source_dir):
        _target_dir = os.path.join(target_dir, os.path.relpath(root, source_dir))

        # Build directories
        os.makedirs(_target_dir, exist_ok=True)

        # Copy each file
        for filename in filenames:
            if filename in [
                "read_env.sh",
            ]:
                continue

            # TODO: ADD FILTERING BASED IN GITIGNORE?
            source_filepath = os.path.join(root, filename)
            target_filepath = os.path.join(_target_dir, filename)

            print(f"Writing {target_filepath}...")

            # Copy file
            shutil.copy2(source_filepath, target_filepath)

            # Update laktory version
            if (
                target_filepath.endswith("requirements.txt")
                or target_filepath.endswith("pyproject.toml")
                or target_filepath.endswith(".py")
                or target_filepath.endswith(".yaml")
                or target_filepath.endswith(".yml")
            ):
                with open(target_filepath, "r") as fp:
                    data = fp.read()

                with open(target_filepath, "w") as fp:
                    fp.write(data.replace("<laktory_version>", VERSION))

    if template == "workflows-dab":
        print(
            """
Sample pipeline files have been written to ./laktory/pipelines/.
A sample DAB job resource has been written to ./resources/.

Add the following to your databricks.yml to enable Laktory:

    variables:
      dab_workspace_root:
        default: ${workspace.root_path}
      laktory_pipelines_dir:
        default: ./laktory/pipelines

    sync:
      paths:
        - ./laktory
      include:
        - ./laktory/.build/**

    include:
      - resources/*.yml

    python:
      venv_path: .venv
      resources:
        - 'laktory.dab:build_resources'

Then deploy with:

    databricks bundle deploy --target dev

See https://www.laktory.ai/concepts/dab/ for full details.
"""
        )
