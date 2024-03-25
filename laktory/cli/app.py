import typer
import os
import yaml
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator
from prompt_toolkit.validation import ValidationError
from prompt_toolkit import prompt
from typing import Union
from typing_extensions import Annotated
from laktory.constants import SUPPORTED_BACKENDS
from laktory.models.stacks.stack import Stack
from laktory.models.basemodel import BaseModel
from laktory._parsers import remove_empty

APP_NAME = "laktory-cli"

DIRPATH = os.path.dirname(__file__)

app = typer.Typer(
    pretty_exceptions_show_locals=False,
    help="Laktory CLI to preview and deploy resources",
)  # prevent display secret data


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


@app.command()
def write(
    backend: Annotated[str, typer.Option(help="IaC backend [terraform]")] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    pulumi_options: Annotated[
        str,
        typer.Option(
            "--pulumi-options", help="Comma separated pulumi options (flags)."
        ),
    ] = None,
    terraform_options: Annotated[
        str,
        typer.Option(
            "--terraform-options", help="Comma separated terraform options (flags)."
        ),
    ] = None,
):
    """
    Write IaC backend file

    Parameters
    ----------
    backend:
        IaC backend [terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory write --env dev
    ```
    """
    controller = CLIController(
        backend=backend,
        organization=organization,
        env=environment,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        raise ValueError("Pulumi backend not supported for init command")
    elif controller.backend == "terraform":
        controller.terraform_call("init")
    else:
        raise ValueError("backend should be ['terraform']")


@app.command()
def init(
    backend: Annotated[str, typer.Option(help="IaC backend [terraform]")] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    pulumi_options: Annotated[
        str,
        typer.Option(
            "--pulumi-options", help="Comma separated pulumi options (flags)."
        ),
    ] = None,
    terraform_options: Annotated[
        str,
        typer.Option(
            "--terraform-options", help="Comma separated terraform options (flags)."
        ),
    ] = None,
):
    """
    Initialize IaC backend

    Parameters
    ----------
    backend:
        IaC backend [terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory init --env dev
    ```

    References
    ----------
    - terraform [init](https://developer.hashicorp.com/terraform/cli/commands/init)
    """
    controller = CLIController(
        backend=backend,
        organization=organization,
        env=environment,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        raise ValueError("Pulumi backend not supported for init command")
    elif controller.backend == "terraform":
        controller.terraform_call("init")
    else:
        raise ValueError("backend should be ['terraform']")


@app.command()
def preview(
    backend: Annotated[
        str, typer.Option(help="IaC backend [pulumi, terraform]")
    ] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    pulumi_options: Annotated[
        str,
        typer.Option(
            "--pulumi-options", help="Comma separated pulumi options (flags)."
        ),
    ] = None,
    terraform_options: Annotated[
        str,
        typer.Option(
            "--terraform-options", help="Comma separated terraform options (flags)."
        ),
    ] = None,
):
    """
    Validate configuration and resources and preview deployment.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory preview --env dev pulumi_options "--show-reads,--show-config"
    ```

    References
    ----------
    - pulumi preview [options](https://www.pulumi.com/docs/cli/commands/pulumi_preview/)
    """
    controller = CLIController(
        backend=backend,
        organization=organization,
        env=environment,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("preview")
    elif controller.backend == "terraform":
        controller.terraform_call("plan")
    else:
        raise ValueError("backend should be ['terraform', 'pulumi']")


@app.command()
def deploy(
    backend: Annotated[
        str, typer.Option(help="IaC backend [pulumi, terraform]")
    ] = None,
    organization: Annotated[
        str,
        typer.Option(
            "--org",
            "-o",
            help="Name of the organization in associated with the pulumi stack.",
        ),
    ] = None,
    environment: Annotated[
        str, typer.Option("--env", "-e", help="Name of the environment")
    ] = None,
    filepath: Annotated[
        str, typer.Option(help="Stack (yaml) filepath.")
    ] = "./stack.yaml",
    pulumi_options: Annotated[
        str,
        typer.Option(
            "--pulumi-options", help="Comma separated pulumi options (flags)."
        ),
    ] = None,
    terraform_options: Annotated[
        str,
        typer.Option(
            "--terraform-options", help="Comma separated terraform options (flags)."
        ),
    ] = None,
):
    """
    Execute deployment.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    organization:
        Name of the organization associated with the Pulumi stack.
    environment:
        Name of the environment.
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory deploy --env dev --filepath my-stack.yaml
    ```

    References
    ----------
    - pulumi up [options](https://www.pulumi.com/docs/cli/commands/pulumi_up/)
    """
    controller = CLIController(
        backend=backend,
        organization=organization,
        env=environment,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("up")
    elif controller.backend == "terraform":
        controller.terraform_call("apply")
    else:
        raise ValueError(f"backend should be {SUPPORTED_BACKENDS}")


class BackendValidator(Validator):
    def validate(self, document):
        text = document.text
        if text.lower() not in SUPPORTED_BACKENDS:
            raise ValidationError(
                message=f"Please enter one of the supported IaC backends {SUPPORTED_BACKENDS}",
                cursor_position=len(text),
            )  # Move cursor to end


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
                        },
                        "columns": [
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
                        ],
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


if __name__ == "__main__":
    app()
