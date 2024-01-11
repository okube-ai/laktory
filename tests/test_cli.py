import os
from laktory import app
from laktory import settings
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
dirpath = os.path.dirname(__file__)


def test_preview_pulumi():
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(
        app, ["preview", "--stack", "okube/dev", "--filepath", filepath]
    )
    assert result.exit_code == 0


def atest_deploy_pulumi():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(
        app,
        [
            "deploy",
            "-s",
            "okube/dev",
            "--filepath",
            filepath,
            "--pulumi-options",
            "--yes",
        ],
    )
    assert result.exit_code == 0

    filepath = os.path.join(dirpath, "stack_empty.yaml")
    result = runner.invoke(
        app,
        [
            "deploy",
            "-s",
            "okube/dev",
            "--filepath",
            filepath,
            "--pulumi-options",
            "--yes",
        ],
    )
    assert result.exit_code == 0


if __name__ == "__main__":
    test_preview_pulumi()
    atest_deploy_pulumi()
