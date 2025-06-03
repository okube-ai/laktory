import pytest
from typer.testing import CliRunner

from laktory import models
from laktory import settings
from laktory._testing import skip_pulumi_preview
from laktory._testing import skip_terraform_plan
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

    # Pulumi requires valid Databricks Host and Token and Pulumi Token to run a preview.
    extras = None
    if template == "unity-catalog":
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
        ["quickstart", "--template", template, "--backend", "terraform"],
    )

    # Read stack
    with open(stack_filepath, "r") as fp:
        stack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)

    # Preview
    stack.init(flags=["-reconfigure"])
    stack.plan()

    # Reset context
    settings.cli_raise_external_exceptions = c0


@pytest.mark.parametrize(
    ["template", "env"],
    [
        ("workflows", "dev"),
        ("workspace", "dev"),
        ("unity-catalog", "global"),
    ],
)
def test_pulumi_preview(monkeypatch, template, env, tmp_path):
    # Set context
    c0 = settings.cli_raise_external_exceptions
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LAKTORY_CLI_RAISE_EXTERNAL_EXCEPTIONS", "true")
    settings.cli_raise_external_exceptions = True

    stack_filepath = tmp_path / "stack.yaml"

    # Pulumi requires valid Databricks Host and Token and Pulumi Token to run a preview.
    extras = None
    if template == "unity-catalog":
        extras = [
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
            "DATABRICKS_ACCOUNT_ID",
        ]
    skip_pulumi_preview(extras=extras)

    # Generate stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", "pulumi"],
    )

    # Read stack
    with open(stack_filepath, "r") as fp:
        stack = models.Stack.model_validate_yaml(fp).to_pulumi(env_name=env)

    # Preview
    # stack.preview(stack=f"okube/{env}")
    stack.preview(stack="okube/dev")

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
