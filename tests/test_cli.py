import os
from laktory import app
from laktory import settings
from laktory import models
from laktory._testing import Paths
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
paths = Paths(__file__)


def test_preview_pulumi():
    filepath = os.path.join(paths.data, "stack.yaml")
    result = runner.invoke(app, ["preview", "--env", "dev", "--filepath", filepath])
    assert result.exit_code == 0


def test_preview_terraform():
    filepath = os.path.join(paths.data, "stack.yaml")

    # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
    with open(filepath, "r") as fp:
        pstack = models.Stack.model_validate_yaml(fp).to_terraform(env_name="dev")
        pstack.init(flags=["-migrate-state", "-upgrade"])

    result = runner.invoke(
        app,
        ["preview", "--backend", "terraform", "--env", "dev", "--filepath", filepath],
    )
    assert result.exit_code == 0


def test_quickstart_pulumi():
    filepath = os.path.join(paths.tmp, "stack_quickstart_pulumi.yaml")
    print(filepath)
    result = runner.invoke(
        app,
        [
            "quickstart",
            "--backend",
            "pulumi",
            "-o",
            "okube",
            "-n",
            "Standard_DS3_v2",
            "--filepath",
            filepath,
        ],
    )

    # TODO: Add validation for the generated stack?
    with open(filepath) as fp:
        stack = models.Stack.model_validate_yaml(fp)

    data = stack.model_dump(exclude_none=True)

    assert stack.backend == "pulumi"
    assert stack.name == "quickstart"
    assert stack.pulumi.config["databricks:host"] == "${vars.DATABRICKS_HOST}"
    assert stack.pulumi.config["databricks:token"] == "${vars.DATABRICKS_TOKEN}"
    assert len(stack.resources.databricks_dbfsfiles) == 1
    assert len(stack.resources.databricks_notebooks) == 1
    assert len(stack.resources.pipelines) == 1


def test_quickstart_terraform():
    filepath = os.path.join(paths.tmp, "stack_quickstart_terraform.yaml")
    result = runner.invoke(
        app,
        [
            "quickstart",
            "--backend",
            "terraform",
            "-o",
            "okube",
            "-n",
            "Standard_DS3_v2",
            "--filepath",
            filepath,
        ],
    )

    with open(filepath) as fp:
        stack = models.Stack.model_validate_yaml(fp)

    data = stack.model_dump(exclude_none=True)

    assert stack.backend == "terraform"
    assert stack.name == "quickstart"
    assert stack.resources.providers["databricks"].host == "${vars.DATABRICKS_HOST}"
    assert stack.resources.providers["databricks"].token == "${vars.DATABRICKS_TOKEN}"
    assert len(stack.resources.databricks_dbfsfiles) == 1
    assert len(stack.resources.databricks_notebooks) == 1
    assert len(stack.resources.pipelines) == 1


def atest_deploy_pulumi():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    for filename in [
        "stack.yaml",
        "stack_empty.yaml",
    ]:
        filepath = os.path.join(paths.data, filename)
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
        filepath = os.path.join(paths.data, filename)
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
    test_quickstart_pulumi()
    test_quickstart_terraform()
    # atest_deploy_pulumi()
    # atest_deploy_terraform()
