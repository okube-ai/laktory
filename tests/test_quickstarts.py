import os

import pytest
from typer.testing import CliRunner

from laktory import models
from laktory import settings
from laktory.cli import app

runner = CliRunner()


@pytest.mark.parametrize(
    ["template", "backend", "env"],
    [
        ("local-pipeline", None, None),
        ("workflows", "terraform", "dev"),
        ("workflows", "pulumi", "dev"),
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
        ("unity-catalog", "terraform", None),
        ("unity-catalog", "pulumi", "global"),
    ],
)
def test_read(monkeypatch, template, backend, env, tmp_path):
    monkeypatch.chdir(tmp_path)

    stack_filepath = tmp_path / "stack.yaml"
    model = models.Stack
    if template == "local-pipeline":
        model = models.Pipeline
        stack_filepath = tmp_path / "pipeline.yaml"

    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", backend],
    )

    # Read Stack
    with open(stack_filepath) as fp:
        stack = model.model_validate_yaml(fp)

    data = stack.model_dump(exclude_unset=True)

    if template == "local-pipeline":
        assert len(stack.nodes) == 6
        return

    resource_keys = list(data["resources"].keys())
    assert stack.backend == backend
    if backend == "pulumi":
        assert stack.organization == "my_organization"
    if template == "workflows":
        target = [
            "databricks_dbfsfiles",
            "databricks_jobs",
            "databricks_notebooks",
            "pipelines",
        ]
    elif template == "workspace":
        target = [
            "databricks_directories",
            "databricks_secretscopes",
            "databricks_warehouses",
        ]
    elif template == "unity-catalog":
        target = ["databricks_catalogs", "databricks_groups", "databricks_users"]
    else:
        raise ValueError(template)

    if backend == "terraform" or template == "unity-catalog":
        target += ["providers"]
    assert set(resource_keys) == set(target)


@pytest.mark.parametrize(
    ["template", "backend", "env"],
    [
        ("workflows", "terraform", "dev"),
        ("workflows", "pulumi", "dev"),
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
        ("unity-catalog", "terraform", None),
        ("unity-catalog", "pulumi", "global"),
    ],
)
def test_preview(monkeypatch, template, backend, env, tmp_path):
    monkeypatch.chdir(tmp_path)

    stack_filepath = tmp_path / "stack.yaml"

    # Set required env vars
    host = os.getenv("DATABRICKS_HOST", "my-host")
    token = os.getenv("DATABRICKS_TOKEN", "my-token")
    monkeypatch.setenv("DATABRICKS_HOST", host)
    monkeypatch.setenv("DATABRICKS_TOKEN", token)
    monkeypatch.setenv("DATABRICKS_HOST_DEV", host)
    monkeypatch.setenv("DATABRICKS_TOKEN_DEV", token)
    c0 = settings.cli_raise_external_exceptions
    settings.cli_raise_external_exceptions = True

    # Missing Databricks Host / Token
    if host == "my-host" or token == "my-token":
        if template == "workflows" and backend == "terraform":
            pytest.skip("Evn variables missing.")

    # Generate stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", backend],
    )

    if backend == "terraform":
        # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
        with open(stack_filepath, "r") as fp:
            pstack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)
            pstack.init(flags=["-migrate-state", "-upgrade"])

    with open(stack_filepath, "r") as fp:
        if backend == "terraform":
            stack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)
            stack.plan()
        else:
            stack = models.Stack.model_validate_yaml(fp).to_pulumi(env_name=env)
            stack.preview(f"okube/{env}")

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
