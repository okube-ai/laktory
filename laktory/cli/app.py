import typer
from typing_extensions import Annotated
from laktory.models.stacks.stack import Stack
from laktory.models.basemodel import BaseModel

APP_NAME = "laktory-cli"

app = typer.Typer(
    pretty_exceptions_show_locals=False  # prevent display secret data
)


class CLIController(BaseModel):
    stack_filepath: str = None
    engine: str = None
    pulumi_stack_name: str = None
    pulumi_options_str: str = None
    terraform_options_str: str = None
    stack: Stack = None

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

        with open(self.filepath, "r") as fp:
            self.stack = Stack.model_validate_yaml(fp)

    def set_engine(self):

        if self.engine is None:
            self.engine = self.stack.engine
        if self.engine is None:
            raise ValueError("engine ['pulumi', 'terraform'] must be specified in stack file or as CLI option ")

    def pulumi_call(self, cmd):
        if self.pulumi_stack_name is None:
            raise ValueError("Argument `stack` must be specified with pulumi engine")

        pstack = self.stack.to_pulumi(env=self.env)
        getattr(pstack, cmd)(stack=self.pulumi_stack_name, flags=self.pulumi_options)


@app.command()
def preview(
        engine: str = None,
        stack: Annotated[str, typer.Option("--stack", "-s")] = None,
        filepath: str = "./stack.yaml",
        pulumi_options: Annotated[str, typer.Option("--pulumi-options")] = None,
        terraform_options: Annotated[str, typer.Option("--terraform-options")] = None,
):

    controller = CLIController(
        cmd="preview",
        engine=engine,
        pulumi_stack_name=stack,
        stack_filepath=filepath,
        pulumi_options=pulumi_options,
        terraform_options=terraform_options,
    )

    # Read Stack
    controller.read_stack()

    # Set engine
    controller.set_engine()

    # Call
    if engine == "pulumi":
        controller.pulumi_call("preview")
    elif engine == "terraform":
        raise NotImplementedError()
    else:
        raise ValueError("engine should be ['terraform', 'pulumi']")


@app.command()
def deploy(
        engine: str = None,
        stack: Annotated[str, typer.Option("--stack", "-s")] = None,
        filepath: str = "./stack.yaml",
        pulumi_options: Annotated[str, typer.Option("--pulumi-options")] = None,
        terraform_options: Annotated[str, typer.Option("--terraform-options")] = None,
):
    controller = CLIController(
        engine=engine,
        pulumi_stack_name=stack,
        stack_filepath=filepath,
        pulumi_options=pulumi_options,
        terraform_options=terraform_options,
    )

    # Read Stack
    controller.read_stack()

    # Set engine
    controller.set_engine()

    # Call
    if engine == "pulumi":
        controller.pulumi_call("up")
    elif engine == "terraform":
        raise NotImplementedError()
    else:
        raise ValueError("engine should be ['terraform', 'pulumi']")


if __name__ == "__main__":
    app()
