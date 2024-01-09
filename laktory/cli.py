from typing import Optional
from laktory.models.stacks.stack import Stack
import typer

app = typer.Typer()


def _read_stack(filepath):

    if filepath is None:
        filepath = "./stack.yaml"

    with open(filepath, "r") as fp:
        stack = Stack.model_validate_yaml(fp)

    return stack


def _pulumi_call(command, stack, stack_name):

    env = None
    if stack_name is not None:
        env = stack_name.split("/")[-1]

    pstack = stack.to_pulumi(env=env)

    getattr(pstack, command)(stack=stack_name)


@app.command()
def preview(stack: str = None, filepath: str = "./stack.yaml"):

    stack_name = stack
    stack = _read_stack(filepath)

    if stack.engine == "pulumi":
        _pulumi_call("preview", stack=stack, stack_name=stack_name)

    elif stack.engine == "terraform":
        raise NotImplementedError()

        # typer.echo(stack.engine)


@app.command()
def deploy(stack: str = None, filepath: str = "./stack.yaml"):

    stack_name = stack
    stack = _read_stack(filepath)

    if stack.engine == "pulumi":
        _pulumi_call("up", stack=stack, stack_name=stack_name)

    elif stack.engine == "terraform":
        raise NotImplementedError()


if __name__ == "__main__":
    app()
