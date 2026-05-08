from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from laktory._settings import settings
from laktory._version import VERSION
from laktory.cli import app

runner = CliRunner()
root = Path(__file__).parent
stack_filepath = str(root / "data" / "stack.yaml")


# --------------------------------------------------------------------------- #
# version                                                                      #
# --------------------------------------------------------------------------- #


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert VERSION in result.output


def test_version_flag():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert VERSION in result.output


# --------------------------------------------------------------------------- #
# validate                                                                     #
# --------------------------------------------------------------------------- #


def test_validate_dev(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    result = runner.invoke(
        app, ["validate", "--env", "dev", "--filepath", stack_filepath]
    )
    assert result.exit_code == 0


def test_validate_prod(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    result = runner.invoke(
        app, ["validate", "--env", "prod", "--filepath", stack_filepath]
    )
    assert result.exit_code == 0


def test_validate_default_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    result = runner.invoke(app, ["validate", "--filepath", stack_filepath])
    assert result.exit_code == 0


def test_validate_missing_file():
    result = runner.invoke(app, ["validate", "--filepath", "/nonexistent/stack.yaml"])
    assert result.exit_code != 0


def test_validate_malformed_yaml(tmp_path):
    bad = tmp_path / "stack.yaml"
    bad.write_text("name: test\norganization: {unclosed\n")
    result = runner.invoke(app, ["validate", "--filepath", str(bad)])
    assert result.exit_code != 0


def test_validate_invalid_model(tmp_path):
    bad = tmp_path / "stack.yaml"
    bad.write_text("name:\n  - not_a_string\norganization: o\n")
    result = runner.invoke(app, ["validate", "--filepath", str(bad)])
    assert result.exit_code != 0


def test_validate_bad_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    result = runner.invoke(
        app, ["validate", "--env", "nonexistent", "--filepath", stack_filepath]
    )
    assert result.exit_code != 0


# --------------------------------------------------------------------------- #
# build                                                                        #
# --------------------------------------------------------------------------- #


def test_build(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    result = runner.invoke(app, ["build", "--env", "dev", "--filepath", stack_filepath])
    assert result.exit_code == 0


# --------------------------------------------------------------------------- #
# init / preview / deploy / destroy  (terraform-mocked)                       #
# --------------------------------------------------------------------------- #


def test_init(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._init.CLIController") as MockController:
        mock_instance = MockController.return_value
        mock_instance.iac_backend = "terraform"
        result = runner.invoke(
            app, ["init", "--env", "dev", "--filepath", stack_filepath]
        )
    assert result.exit_code == 0
    mock_instance.terraform_call.assert_called_once_with("init")


def test_preview(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._preview.CLIController") as MockController:
        mock_instance = MockController.return_value
        mock_instance.iac_backend = "terraform"
        result = runner.invoke(
            app, ["preview", "--env", "dev", "--filepath", stack_filepath]
        )
    assert result.exit_code == 0
    mock_instance.terraform_call.assert_called_once_with("plan")


def test_deploy(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._deploy.CLIController") as MockController:
        mock_instance = MockController.return_value
        mock_instance.iac_backend = "terraform"
        result = runner.invoke(
            app, ["deploy", "--env", "dev", "--filepath", stack_filepath]
        )
    assert result.exit_code == 0
    mock_instance.terraform_call.assert_called_once_with("apply")


def test_deploy_auto_approve(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._deploy.CLIController") as MockController:
        mock_instance = MockController.return_value
        mock_instance.iac_backend = "terraform"
        result = runner.invoke(
            app,
            ["deploy", "--yes", "--env", "dev", "--filepath", stack_filepath],
        )
    assert result.exit_code == 0
    _, kwargs = MockController.call_args
    assert kwargs.get("auto_approve") is True


def test_destroy(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._destroy.CLIController") as MockController:
        mock_instance = MockController.return_value
        mock_instance.iac_backend = "terraform"
        result = runner.invoke(
            app, ["destroy", "--env", "dev", "--filepath", stack_filepath]
        )
    assert result.exit_code == 0
    mock_instance.terraform_call.assert_called_once_with("destroy")


# --------------------------------------------------------------------------- #
# run  (Dispatcher-mocked)                                                     #
# --------------------------------------------------------------------------- #


def test_run_job(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._run.Dispatcher") as MockDispatcher:
        mock_dispatcher = MockDispatcher.return_value
        result = runner.invoke(
            app,
            [
                "run",
                "--env",
                "dev",
                "--filepath",
                stack_filepath,
                "--dbks-job",
                "job-stock-prices-ut-stack",
            ],
        )
    assert result.exit_code == 0
    mock_dispatcher.get_resource_ids.assert_called_once()
    mock_dispatcher.run_databricks_job.assert_called_once_with(
        job_name="job-stock-prices-ut-stack",
        timeout=1200,
        raise_exception=True,
        current_run_action="WAIT",
    )


def test_run_pipeline(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._run.Dispatcher") as MockDispatcher:
        mock_dispatcher = MockDispatcher.return_value
        result = runner.invoke(
            app,
            [
                "run",
                "--env",
                "dev",
                "--filepath",
                stack_filepath,
                "--dbks-pipeline",
                "pl-stock-prices-ut-stack",
            ],
        )
    assert result.exit_code == 0
    mock_dispatcher.get_resource_ids.assert_called_once()
    mock_dispatcher.run_databricks_dlt.assert_called_once_with(
        dlt_name="pl-stock-prices-ut-stack",
        timeout=1200,
        raise_exception=True,
        current_run_action="WAIT",
        full_refresh=False,
    )


def test_run_full_refresh(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._run.Dispatcher") as MockDispatcher:
        mock_dispatcher = MockDispatcher.return_value
        result = runner.invoke(
            app,
            [
                "run",
                "--env",
                "dev",
                "--filepath",
                stack_filepath,
                "--dbks-pipeline",
                "pl-stock-prices-ut-stack",
                "--full-refresh",
            ],
        )
    assert result.exit_code == 0
    _, kwargs = mock_dispatcher.run_databricks_dlt.call_args
    assert kwargs.get("full_refresh") is True


def test_run_no_target(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._run.Dispatcher"):
        result = runner.invoke(
            app,
            ["run", "--env", "dev", "--filepath", stack_filepath],
        )
    assert result.exit_code != 0


def test_run_both_targets(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")
    with patch("laktory.cli._run.Dispatcher"):
        result = runner.invoke(
            app,
            [
                "run",
                "--env",
                "dev",
                "--filepath",
                stack_filepath,
                "--dbks-job",
                "job-stock-prices-ut-stack",
                "--dbks-pipeline",
                "pl-stock-prices-ut-stack",
            ],
        )
    assert result.exit_code != 0
