import os

import pytest
from typer.testing import CliRunner

from laktory import models
from laktory import settings
from laktory._testing import skip_terraform_plan
from laktory.cli import app

runner = CliRunner()


@pytest.mark.parametrize(
    ["template", "env"],
    [
        ("local-pipeline", None),
        ("workflows", "dev"),
        ("workspace", "dev"),
        ("unity-catalog", None),
    ],
)
def test_read(monkeypatch, template, env, tmp_path):
    monkeypatch.chdir(tmp_path)

    stack_filepath = tmp_path / "stack.yaml"
    model = models.Stack
    if template == "local-pipeline":
        model = models.Pipeline
        stack_filepath = tmp_path / "pipeline.yaml"

    _ = runner.invoke(
        app,
        ["quickstart", "--template", template],
    )

    # Read Stack
    with open(stack_filepath) as fp:
        stack = model.model_validate_yaml(fp)

    data = stack.model_dump(exclude_unset=True)

    if template == "local-pipeline":
        assert len(stack.nodes) == 6
        return

    resource_keys = list(data["resources"].keys())
    assert stack.backend == "terraform"
    if template == "workflows":
        target = [
            "databricks_dbfsfiles",
            "databricks_jobs",
            "databricks_pythonpackages",
            "databricks_workspacetrees",
            "pipelines",
            "providers",
        ]
    elif template == "workspace":
        target = [
            "databricks_directories",
            "databricks_secretscopes",
            "databricks_warehouses",
            "providers",
        ]
    elif template == "unity-catalog":
        target = [
            "databricks_catalogs",
            "databricks_groups",
            "databricks_users",
            "providers",
        ]
    else:
        raise ValueError(template)

    assert set(resource_keys) == set(target)


@pytest.mark.parametrize(
    ["template", "env"],
    [
        ("workflows", "dev"),
        ("workspace", "dev"),
        ("unity-catalog", None),
    ],
)
def test_terraform_plan(monkeypatch, template, env, tmp_path):
    # Set context
    c0 = settings.cli_raise_external_exceptions
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LAKTORY_CLI_RAISE_EXTERNAL_EXCEPTIONS", "true")
    settings.cli_raise_external_exceptions = True

    stack_filepath = tmp_path / "stack.yaml"

    extras = None
    if template == "unity-catalog":
        monkeypatch.setenv("DATABRICKS_HOST_DEV", os.getenv("DATABRICKS_HOST"))
        monkeypatch.setenv("DATABRICKS_TOKEN_DEV", os.getenv("DATABRICKS_TOKEN"))
        extras = [
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
            "DATABRICKS_ACCOUNT_ID",
        ]
    skip_terraform_plan(extras=extras)

    # Generate stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template],
    )

    # Read stack
    with open(stack_filepath, "r") as fp:
        stack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)

    # Preview
    stack.init(flags=["-reconfigure"])
    stack.plan()

    # Reset context
    settings.cli_raise_external_exceptions = c0


def test_run(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    # Run Quickstart
    _ = runner.invoke(
        app,
        ["quickstart", "--template", "local-pipeline"],
    )

    # Run Scripts
    for filename in [
        "00_explore_pipeline.py",
        "01_execute_node_bronze.py",
        "02_execute_node_silver.py",
        "03_execute_pipeline.py",
        "04_code_pipeline.py",
    ]:
        with open(filename, "r") as f:
            print(f"----- Executing {filename}")
            code = f.read()
            exec(code)
            print("")
            print("----- Execution completed\n\n")
