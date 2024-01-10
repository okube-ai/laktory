from laktory import app
from laktory import settings
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True


def test_preview_pulumi():
    result = runner.invoke(app, ["preview", "--stack", "okube/dev"])
    assert result.exit_code == 0


def test_deploy_pulumi():
    result = runner.invoke(app, ["deploy", "-s", "okube/dev", "--pulumi-options", "--yes"])
    assert result.exit_code == 0

    result = runner.invoke(app, ["deploy", "-s", "okube/dev", "--filepath", "./stack_empty.yaml", "--pulumi-options", "--yes"])
    assert result.exit_code == 0


if __name__ == "__main__":
    test_preview_pulumi()
    test_deploy_pulumi()
