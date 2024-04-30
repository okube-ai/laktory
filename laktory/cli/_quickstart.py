import os
import typer
import yaml
from typing import Annotated
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit import prompt

from laktory.cli._common import BackendValidator
from laktory.cli._common import DIRPATH
from laktory.cli.app import app
from laktory._parsers import remove_empty
from laktory.models.stacks.stack import Stack
from laktory.constants import SUPPORTED_BACKENDS


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

    if not os.path.exists("./notebooks/pipelines"):
        os.makedirs("./notebooks/pipelines")
    if not os.path.exists("./data"):
        os.makedirs("./data")

    # Copy notebooks
    for filename in [
        "dlt_brz_template.py",
        "dlt_slv_template.py",
    ]:
        with open(os.path.join(DIRPATH, f"../resources/notebooks/{filename}")) as fp:
            data = fp.read()
            data = data.replace("pl-stock-prices", "pl-quickstart")

        with open(f"./notebooks/pipelines/{filename}", "w") as fp:
            fp.write(data)

    # Copy data
    for filename in [
        "stock_prices.json",
    ]:
        with open(os.path.join(DIRPATH, f"../resources/data/{filename}")) as fp:
            data = fp.read()

        with open(f"./data/{filename}", "w") as fp:
            fp.write(data)

    # Build providers (terraform only)
    providers = {
        "databricks": {
            "host": "${vars.DATABRICKS_HOST}",
            "token": "${vars.DATABRICKS_TOKEN}",
        }
    }

    # Build resources
    resources = {
        "notebooks": {
            "notebook-pipelines-dlt-brz-template": {
                "source": "./notebooks/pipelines/dlt_brz_template.py"
            },
            "notebook-pipelines-dlt-slv-template": {
                "source": "./notebooks/pipelines/dlt_slv_template.py"
            },
        },
        "dbfsfiles": {
            "dbfs-file-stock-prices": {
                "source": "./data/stock_prices.json",
                "path": "/Workspace/.laktory/landing/events/yahoo-finance/stock_price/stock_prices.json",
            },
        },
        "pipelines": {
            "pl-quickstart": {
                "name": "pl-quickstart",
                "target": "default",
                "development": "${vars.is_dev}",
                "configuration": {"pipeline_name": "pl-quickstart"},
                "clusters": [
                    {
                        "name": "default",
                        "node_type_id": node_type,
                        "autoscale": {
                            "min_workers": 1,
                            "max_workers": 2,
                        },
                    },
                ],
                "libraries": [
                    {"notebook": {"path": "/.laktory/pipelines/dlt_brz_template.py"}},
                    {"notebook": {"path": "/.laktory/pipelines/dlt_slv_template.py"}},
                ],
                "tables": [
                    {
                        "name": "brz_stock_prices",
                        "builder": {
                            "layer": "BRONZE",
                            "event_source": {
                                "name": "stock_price",
                                "events_root": "dbfs:/Workspace/.laktory/landing/events/",
                                "producer": {
                                    "name": "yahoo-finance",
                                },
                            },
                        },
                    },
                    {
                        "name": "slv_stock_prices",
                        "expectations": [
                            {
                                "name": "positive_price",
                                "expression": "open > 0",
                                "action": "FAIL",
                            }
                        ],
                        "builder": {
                            "layer": "SILVER",
                            "table_source": {
                                "name": "brz_stock_prices",
                            },
                            "spark_chain": {
                                "nodes": [
                                    {
                                        "name": "created_at",
                                        "type": "timestamp",
                                        "sql_expression": "data.created_at",
                                    },
                                    {
                                        "name": "symbol",
                                        "type": "string",
                                        "spark_func_name": "coalesce",
                                        "spark_func_args": ["data._created_at"],
                                    },
                                    {
                                        "name": "open",
                                        "type": "double",
                                        "sql_expression": "data.open",
                                    },
                                    {
                                        "name": "close",
                                        "type": "double",
                                        "sql_expression": "data.close",
                                    },
                                ]
                            },
                        },
                    },
                ],
            }
        },
    }

    # Build environments
    environments = {
        "dev": {"variables": {"env": "dev", "is_dev": True}},
    }

    # Build stack
    if backend == "pulumi":
        stack = Stack(
            organization=organization,
            name=stack,
            backend=backend,
            pulumi={
                "config": {
                    "databricks:host": "${vars.DATABRICKS_HOST}",
                    "databricks:token": "${vars.DATABRICKS_TOKEN}",
                }
            },
            resources=resources,
            environments=environments,
        )
        stack.pulumi.outputs = None

    else:
        resources["providers"] = providers
        stack = Stack(
            organization=organization,
            name=stack,
            backend=backend,
            resources=resources,
            environments=environments,
        )

    # Dump data
    data = stack.model_dump(exclude_none=True)
    data = remove_empty(data)

    # Clean up resources
    if backend != "pulumi" and "pulumi" in data.keys():
        del data["pulumi"]
    for k, n in data["resources"]["notebooks"].items():
        del data["resources"]["notebooks"][k]["path"]

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
