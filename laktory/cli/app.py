import typer
from typing import Union
from typing_extensions import Annotated
from laktory.models.stacks.stack import Stack
from laktory.models.basemodel import BaseModel

APP_NAME = "laktory-cli"

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
        raise ValueError("backend should be ['terraform', 'pulumi']")


if __name__ == "__main__":
    app()
