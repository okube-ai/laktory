import typer
import yaml
import os
from typing import Annotated
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit import prompt

from laktory.cli._common import BackendValidator
from laktory.cli._common import DIRPATH
from laktory.cli.app import app
from laktory._parsers import remove_empty
from laktory.models.stacks.stack import Stack
from laktory.constants import SUPPORTED_BACKENDS


def read_template():

    filepath = os.path.join(os.path.dirname(__file__), "quickstart_stack.yaml")

    with open(filepath, "r") as fp:
        stack = Stack.model_validate_yaml(fp)

    return stack


@app.command()
def quickstart(
    backend: Annotated[
        str, typer.Option("--backend", "-b", help="IaC backend [terraform, terraform]")
    ] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
    node_type: Annotated[
        str,
        typer.Option(
            "--node",
            "-n",
            help="Type of nodes for compute (e.g.: Standard_DS3_v2, c5.2xlarge,)",
        ),
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
):
    """
    Build get started stack in the calling directory.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    node_type:
        Type of nodes for compute (e.g.: Standard_DS3_v2, c5.2xlarge,).
    filepath:
        Filepath of the generated stack yaml file.

    Examples
    --------
    ```cmd
    laktory quickstart
    ```
    """

    # Backend
    completer = WordCompleter(SUPPORTED_BACKENDS, ignore_case=True)
    if backend is None:
        backend = prompt(
            f"Select IaC backend {SUPPORTED_BACKENDS}: ",
            completer=completer,
            validator=BackendValidator(),
        )

    # Stack
    stack = "quickstart"

    # Organization
    if backend == "pulumi":
        if organization is None:
            organization = prompt(f"Pulumi Organization: ")
        if stack is None:
            stack = prompt(f"Stack Name (must match Pulumi project name): ")
    elif backend == "terraform":
        if stack is None:
            stack = prompt(f"Stack Name: ")
    else:
        raise ValueError(f"Supported backends are {SUPPORTED_BACKENDS}")

    # Node type id
    if node_type is None:
        node_type = prompt(
            f"Node type for pipeline (e.g.: Standard_DS3_v2, c5.2xlarge, etc.): "
        )

    if not os.path.exists("./notebooks/dlt"):
        os.makedirs("./notebooks/dlt")
    if not os.path.exists("./data"):
        os.makedirs("./data")

    # Copy notebooks
    for filename in [
        "dlt_laktory_pl.py",
    ]:
        with open(os.path.join(DIRPATH, f"../resources/notebooks/{filename}")) as fp:
            data = fp.read()
            data = data.replace("pl-stock-prices", "pl-quickstart")

        with open(f"./notebooks/dlt/{filename}", "w") as fp:
            fp.write(data)

    # Copy data
    for filename in [
        "stock_prices.json",
    ]:
        with open(os.path.join(DIRPATH, f"../resources/data/{filename}")) as fp:
            data = fp.read()

        with open(f"./data/{filename}", "w") as fp:
            fp.write(data)

    # Read template stack
    template_stack = read_template()
    template_stack.backend = backend
    template_stack.resources.pipelines["pl-quickstart"].dlt.clusters[
        0
    ].node_type_id = node_type

    # Build stack
    if backend == "pulumi":

        template_stack.resources.providers = {}
        stack = Stack(
            organization=template_stack.organization,
            name=stack,
            backend=backend,
            pulumi={
                "config": {
                    "databricks:host": "${vars.DATABRICKS_HOST}",
                    "databricks:token": "${vars.DATABRICKS_TOKEN}",
                }
            },
            resources=template_stack.resources,
            environments=template_stack.environments,
        )
        stack.pulumi.outputs = None

    else:
        stack = Stack(
            organization=template_stack.organization,
            name=stack,
            backend=backend,
            resources=template_stack.resources,
            environments=template_stack.environments,
        )

    # Dump data
    data = stack.model_dump(exclude_none=True)
    data = remove_empty(data)

    # Clean up resources
    if backend != "pulumi" and "pulumi" in data.keys():
        del data["pulumi"]
    for k, n in data["resources"]["databricks_notebooks"].items():
        del data["resources"]["databricks_notebooks"][k]["path"]

    # Sort data
    d = {}
    for k in [
        "name",
        "organization",
        "backend",
        "pulumi",
        "resources",
        "environments",
    ]:
        if k in data:
            d[k] = data[k]

    for k in data.keys():
        if k not in d:
            d[k] = data[k]

    # Output Stack
    with open(filepath, "w") as fp:
        yaml.safe_dump(d, fp, sort_keys=False)
