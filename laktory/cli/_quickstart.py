import typer
import shutil
import os
from typing import Annotated
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit import prompt

from laktory.cli._common import BackendValidator
from laktory.cli._common import TemplateValidator
from laktory.cli.app import app
from laktory.constants import QUICKSTART_TEMPLATES
from laktory.constants import SUPPORTED_BACKENDS
from laktory._version import VERSION


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
    backend: Annotated[
        str, typer.Option("--backend", "-b", help="IaC backend [terraform, pulumi]")
    ] = None,
):
    """
    Build get started stack in the calling directory.

    Parameters
    ----------
    template:
        Stack template [unity-catalog, workspace, workflows]
    backend:
        IaC backend [pulumi, terraform]

    Examples
    --------
    ```cmd
    laktory quickstart
    ```
    """

    # Template
    completer = WordCompleter(QUICKSTART_TEMPLATES, ignore_case=True)
    if template is None:
        template = prompt(
            f"Select template {QUICKSTART_TEMPLATES}: ",
            completer=completer,
            validator=TemplateValidator(),
        )

    # Backend
    completer = WordCompleter(SUPPORTED_BACKENDS, ignore_case=True)
    if backend is None and template not in ["local-pipeline"]:
        backend = prompt(
            f"Select IaC backend {SUPPORTED_BACKENDS}: ",
            completer=completer,
            validator=BackendValidator(),
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
                "stack.yaml",  # stack_terra.yaml or stack_pulumi.yaml will be used instead
            ]:
                continue

            # TODO: ADD FILTERING BASED IN GITIGNORE?

            source_filepath = os.path.join(root, filename)
            target_filepath = os.path.join(_target_dir, filename)

            print(f"Writing {target_filepath}...")

            # Rename stack files
            if filename == "stack_pulumi.yaml":
                if backend == "terraform":
                    continue
                else:
                    target_filepath = target_filepath.replace(
                        "stack_pulumi.yaml", "stack.yaml"
                    )

            elif filename == "stack_terra.yaml":
                if backend == "pulumi":
                    continue
                else:
                    target_filepath = target_filepath.replace(
                        "stack_terra.yaml", "stack.yaml"
                    )

            # Copy file
            shutil.copy2(source_filepath, target_filepath)

            # Update laktory version
            if target_filepath.endswith("requirements.txt") or target_filepath.endswith(
                ".py"
            ):
                with open(target_filepath, "r") as fp:
                    data = fp.read()

                with open(target_filepath, "w") as fp:
                    fp.write(data.replace("<laktory_version>", VERSION))

            if backend == "pulumi" and target_filepath.endswith("catalogs.yaml"):
                with open(target_filepath, "r") as fp:
                    data = fp.read()

                with open(target_filepath, "w") as fp:
                    fp.write(
                        data.replace(
                            "provider: ${resources.databricks.",
                            "provider: ${resources.provider-databricks-",
                        )
                    )
