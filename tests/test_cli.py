import os
import shutil
import uuid

from laktory import app
from laktory import settings
from laktory import models
from laktory._testing import Paths
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
paths = Paths(__file__)


def _read_stack(template, backend):

    dirpath = os.path.join(paths.tmp, f"quickstart_{template}_{backend}_{str(uuid.uuid4())}")
    stack_filepath = os.path.join(dirpath, "stack.yaml")

    # Change dir
    os.mkdir(dirpath)
    os.chdir(dirpath)

    # Copy Stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", backend],
    )

    # Read Stack
    with open(stack_filepath) as fp:
        stack = models.Stack.model_validate_yaml(fp)
    data = stack.model_dump()

    # Count resources
    resources_count = 0
    for k in data["resources"]:
        if k == "providers":
            continue
        resources_count += len(data["resources"][k].keys())

    if template == "workspace":
        assert resources_count == 7
        assert "directory-laktory-dashboards" in data["resources"]["databricks_directories"]
        assert "secret-scope-laktory" in data["resources"]["databricks_secretscopes"]
        assert "warehouse-laktory" in data["resources"]["databricks_warehouses"]
        pass
    elif template == "workflows":
        assert resources_count == 4
        assert "dbfs-file-stock-prices" in data["resources"]["databricks_dbfsfiles"]
        assert "notebook-job-laktory-pl" in data["resources"]["databricks_notebooks"]
        assert "notebook-dlt-laktory-pl" in data["resources"]["databricks_notebooks"]
        assert "pl-quickstart" in data["resources"]["pipelines"]
    else:
        raise ValueError()

    # Cleanup
    shutil.rmtree(dirpath)


def _preview_stack(template, backend, env):

    dirpath = os.path.join(paths.tmp, f"quickstart_{template}_{backend}_{str(uuid.uuid4())}")
    stack_filepath = os.path.join(dirpath, "stack.yaml")

    # Change dir
    os.mkdir(dirpath)
    os.chdir(dirpath)

    # Copy Stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", backend],
    )

    if backend == "terraform":
        # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
        with open(stack_filepath, "r") as fp:
            pstack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)
            pstack.init(flags=["-migrate-state", "-upgrade"])

    # Preview Stack
    cmds = ["preview"]
    if env:
        cmds += ["--env", env]
    _ = runner.invoke(app, cmds)

    # Cleanup
    shutil.rmtree(dirpath)


def _deploy_stack(template, backend, env):

    dirpath = os.path.join(paths.tmp, f"quickstart_{template}_{backend}_{str(uuid.uuid4())}")
    stack_filepath = os.path.join(dirpath, "stack.yaml")

    # Change dir
    os.mkdir(dirpath)
    os.chdir(dirpath)

    # Copy Stack
    _ = runner.invoke(
        app,
        ["quickstart", "--template", template, "--backend", backend],
    )

    if backend == "terraform":
        # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
        with open(stack_filepath, "r") as fp:
            pstack = models.Stack.model_validate_yaml(fp).to_terraform(env_name=env)
            pstack.init(flags=["-migrate-state", "-upgrade"])

    # Deploy Stack
    cmds = ["deploy"]
    if env:
        cmds += ["-e", env]
    if backend == "pulumi":
        cmds += ["--pulumi-options", "--yes"]
    elif backend == "terraform":
        cmds += ["--terraform-options", "--auto-approve"]
    _ = runner.invoke(app, cmds)

    # Cleanup
    shutil.rmtree(dirpath)


def test_read_quickstart_stacks():
    for template, backend in [
        ("workspace", "terraform"),
        ("workspace", "pulumi"),
        ("workflows", "terraform"),
        ("workflows", "pulumi"),
    ]:
        _read_stack(template, backend)


def atest_preview_quickstart_stacks():
    for template, backend, env in [
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
    ]:
        _preview_stack(template, backend, env)


def atest_deploy_quickstart_stacks():
    for template, backend, env in [
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
    ]:
        _deploy_stack(template, backend, env)


if __name__ == "__main__":
    test_read_quickstart_stacks()
    test_preview_quickstart_stacks()
    atest_deploy_quickstart_stacks()
