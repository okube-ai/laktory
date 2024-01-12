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
    pulumi_stack_name: Union[str, None] = None
    pulumi_options_str: Union[str, None] = None
    terraform_options_str: Union[str, None] = None
    stack: Union[Stack, None] = None

    @property
    def pulumi_options(self):
        if self.pulumi_options_str is None:
            return None
        return self.pulumi_options_str.split(",")

    @property
    def terraform_options(self):
        if self.terraform_options_str is None:
            return None
        return self.pulumi_options_str.split(",")

    @property
    def env(self):
        env = None
        if self.pulumi_stack_name is not None:
            env = self.pulumi_stack_name.split("/")[-1]
        return env

    def read_stack(self):
        if self.stack_filepath is None:
            self.stack_filepath = "./stack.yaml"

        with open(self.stack_filepath, "r") as fp:
            self.stack = Stack.model_validate_yaml(fp)

    def set_backend(self):
        if self.backend is None:
            self.backend = self.stack.backend
        if self.backend is None:
            raise ValueError(
                "backend ['pulumi', 'terraform'] must be specified in stack file or as CLI option "
            )

    def pulumi_call(self, cmd):
        if self.pulumi_stack_name is None:
            raise ValueError("Argument `stack` must be specified with pulumi backend")

        pstack = self.stack.to_pulumi(env=self.env)
        getattr(pstack, cmd)(stack=self.pulumi_stack_name, flags=self.pulumi_options)


@app.command()
def preview(
    backend: Annotated[str, typer.Option(
        help="IaC backend [pulumi, terraform]"
    )] = None,
    stack: Annotated[str, typer.Option(
        "--stack", "-s",
        help="Name of the stack. With pulumi backend, expected format is {organization}/{stack}"
    )] = None,
    filepath: Annotated[str, typer.Option(
        help="Stack (yaml) filepath."
    )] = "./stack.yaml",
    pulumi_options: Annotated[str, typer.Option(
        "--pulumi-options",
        help="Comma separated pulumi options (flags)."
    )] = None,
    terraform_options: Annotated[str, typer.Option(
        "--terraform-options",
        help="Comma separated terraform options (flags)."
    )] = None,
):
    """
    Validate configuration and resources and preview deployment.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    stack:
        Name of the stack. With pulumi backend, expected format is {organization}/{stack}
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory preview -s okube/dev pulumi_options "--show-reads,--show-config"
    ```

    References
    ----------
    - pulumi preview [options](https://www.pulumi.com/docs/cli/commands/pulumi_preview/)
    """
    controller = CLIController(
        backend=backend,
        pulumi_stack_name=stack,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Read Stack
    controller.read_stack()

    # Set backend
    controller.set_backend()

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("preview")
    elif controller.backend == "terraform":
        raise NotImplementedError()
    else:
        raise ValueError("backend should be ['terraform', 'pulumi']")


@app.command()
def deploy(
    backend: Annotated[str, typer.Option(
        help="IaC backend [pulumi, terraform]"
    )] = None,
    stack: Annotated[str, typer.Option(
        "--stack", "-s",
        help="Name of the stack. With pulumi backend, expected format is {organization}/{stack}"
    )] = None,
    filepath: Annotated[str, typer.Option(
        help="Stack (yaml) filepath."
    )] = "./stack.yaml",
    pulumi_options: Annotated[str, typer.Option(
        "--pulumi-options",
        help="Comma separated pulumi options (flags)."
    )] = None,
    terraform_options: Annotated[str, typer.Option(
        "--terraform-options",
        help="Comma separated terraform options (flags)."
    )] = None,
):
    """
    Execute deployment.

    Parameters
    ----------
    backend:
        IaC backend [pulumi, terraform]
    stack:
        Name of the stack. With pulumi backend, expected format is {organization}/{stack}
    filepath:
        Stack (yaml) filepath.
    pulumi_options:
        Comma separated pulumi options (flags).
    terraform_options:
        Comma separated terraform options (flags).

    Examples
    --------
    ```cmd
    laktory deploy -s okube/dev --filepath my-stack.yaml
    ```

    References
    ----------
    - pulumi up [options](https://www.pulumi.com/docs/cli/commands/pulumi_up/)
    """
    controller = CLIController(
        backend=backend,
        pulumi_stack_name=stack,
        stack_filepath=filepath,
        pulumi_options_str=pulumi_options,
        terraform_options_str=terraform_options,
    )

    # Read Stack
    controller.read_stack()

    # Set backend
    controller.set_backend()

    # Call
    if controller.backend == "pulumi":
        controller.pulumi_call("up")
    elif controller.backend == "terraform":
        raise NotImplementedError()
    else:
        raise ValueError("backend should be ['terraform', 'pulumi']")


if __name__ == "__main__":
    app()
