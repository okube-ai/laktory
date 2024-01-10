import os
from laktory import app
from laktory import settings
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
dirpath = os.path.dirname(__file__)


def test_preview_pulumi():
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(app, ["preview", "--stack", "okube/dev", "--filepath", filepath])
    assert result.exit_code == 0


def test_deploy_pulumi():
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(app, ["deploy", "-s", "okube/dev", "--filepath", filepath, "--pulumi-options", "--yes"])
    assert result.exit_code == 0

    filepath = os.path.join(dirpath, "stack_empty.yaml")
    result = runner.invoke(app, ["deploy", "-s", "okube/dev", "--filepath", filepath, "--pulumi-options", "--yes"])
    assert result.exit_code == 0


if __name__ == "__main__":
    test_preview_pulumi()
    test_deploy_pulumi()
