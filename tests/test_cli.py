import os
from laktory import app
from laktory import settings
from laktory import models
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
dirpath = os.path.dirname(__file__)


def test_preview_pulumi():
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(app, ["preview", "--env", "dev", "--filepath", filepath])
    assert result.exit_code == 0


def test_preview_terraform():
    filepath = os.path.join(dirpath, "stack.yaml")

    # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
    with open(filepath, "r") as fp:
        pstack = models.Stack.model_validate_yaml(fp).to_terraform(env="dev")
        pstack.init(flags=["-migrate-state"])

    result = runner.invoke(
        app,
        ["preview", "--backend", "terraform", "--env", "dev", "--filepath", filepath],
    )
    assert result.exit_code == 0


def atest_deploy_pulumi():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    for filename in [
        "stack.yaml",
        "stack_empty.yaml",
    ]:
        filepath = os.path.join(dirpath, filename)
        result = runner.invoke(
            app,
            [
                "deploy",
                "-e",
                "dev",
                "--filepath",
                filepath,
                "--pulumi-options",
                "--yes",
            ],
        )
        assert result.exit_code == 0


def atest_deploy_terraform():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    for filename in [
        "stack.yaml",
        "stack_empty.yaml",
    ]:
        filepath = os.path.join(dirpath, filename)
        result = runner.invoke(
            app,
            [
                "deploy",
                "-e",
                "dev",
                "--backend",
                "terraform",
                "--filepath",
                filepath,
                "--terraform-options",
                "--auto-approve",
            ],
        )
        assert result.exit_code == 0


if __name__ == "__main__":
    test_preview_pulumi()
    test_preview_terraform()
    atest_deploy_pulumi()
    atest_deploy_terraform()
