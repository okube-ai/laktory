import os
import shutil
import uuid
from pathlib import Path
from py import path as pypath

from laktory import app
from laktory import settings
from laktory import models
from laktory._testing import Paths
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
paths = Paths(__file__)


def _read_stack(template, backend):

    dirpath = pypath.local(
        f"{paths.tmp}/quickstart_{template}_{backend}_{str(uuid.uuid4())}"
    )
    stack_filepath = dirpath / "stack.yaml"

    # Change dir
    os.mkdir(dirpath)

    # Copy Stack
    with dirpath.as_cwd():
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
        assert (
            "directory-laktory-dashboards"
            in data["resources"]["databricks_directories"].keys()
        )
        assert (
            "secret-scope-laktory"
            in data["resources"]["databricks_secretscopes"].keys()
        )
        assert "warehouse-laktory" in data["resources"]["databricks_warehouses"].keys()
        pass
    elif template == "workflows":
        assert resources_count == 7
        assert (
            "dbfs-file-stock-prices" in data["resources"]["databricks_dbfsfiles"].keys()
        )
        assert (
            "notebook-job-laktory-pl"
            in data["resources"]["databricks_notebooks"].keys()
        )
        assert (
            "notebook-dlt-laktory-pl"
            in data["resources"]["databricks_notebooks"].keys()
        )
        assert "job-hello" in data["resources"]["databricks_jobs"].keys()
        assert "pl-stocks-job" in data["resources"]["pipelines"].keys()
        assert "pl-stocks-dlt" in data["resources"]["pipelines"].keys()
    elif template == "unity-catalog":
        assert resources_count == 8
        assert "group-laktory-friends" in data["resources"]["databricks_groups"].keys()
        assert "user-kubic-okube-ai" in data["resources"]["databricks_users"].keys()
        assert "catalog-lqs-prd" in data["resources"]["databricks_catalogs"].keys()

    else:
        raise ValueError()

    # Cleanup
    shutil.rmtree(dirpath)


def _preview_stack(template, backend, env):

    dirpath = pypath.local(
        f"{paths.tmp}/quickstart_{template}_{backend}_{str(uuid.uuid4())}"
    )
    stack_filepath = dirpath / "stack.yaml"

    # Change dir
    os.mkdir(dirpath)

    # Copy Stack
    with dirpath.as_cwd():
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

    dirpath = pypath.local(
        f"{paths.tmp}/quickstart_{template}_{backend}_{str(uuid.uuid4())}"
    )
    stack_filepath = dirpath / "stack.yaml"

    # Change dir
    os.mkdir(dirpath)

    # Copy Stack
    with dirpath.as_cwd():
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
        ("unity-catalog", "terraform"),
        ("unity-catalog", "pulumi"),
    ]:
        _read_stack(template, backend)


def test_preview_quickstart_stacks():
    for template, backend, env in [
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
        ("workflows", "terraform", "dev"),
        ("workflows", "pulumi", "dev"),
        ("unity-catalog", "terraform", None),
        ("unity-catalog", "pulumi", None),
    ]:
        _preview_stack(template, backend, env)


def atest_deploy_quickstart_stacks():
    for template, backend, env in [
        ("workspace", "terraform", "dev"),
        ("workspace", "pulumi", "dev"),
        ("workflows", "terraform", "dev"),
        ("workflows", "pulumi", "dev"),
        ("unity-catalog", "terraform"),
        ("unity-catalog", "pulumi"),
    ]:
        _deploy_stack(template, backend, env)


def test_quickstart_localpipeline():

    dirpath = pypath.local(f"{paths.tmp}/quickstart_local_pipeline_{str(uuid.uuid4())}")
    # stack_filepath = dirpath / "stack.yaml"

    # Change dir
    os.mkdir(dirpath)

    with dirpath.as_cwd():

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
                print(f"----- Execution completed\n\n")

    # Cleanup
    shutil.rmtree(dirpath)


if __name__ == "__main__":
    test_read_quickstart_stacks()
    test_preview_quickstart_stacks()
    atest_deploy_quickstart_stacks()
    test_quickstart_localpipeline()
