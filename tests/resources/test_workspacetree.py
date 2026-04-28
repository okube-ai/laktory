import os
from pathlib import Path

import laktory as lk
from laktory._testing import Paths
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan

paths = Paths(__file__)


def get_workspace_tree():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    return lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath),
    )


def test_workspace_tree():
    tree = get_workspace_tree()

    resources = tree.core_resources
    assert len(resources) == 6

    for r in resources:
        print(r.source)

    r = resources[0]
    assert r.source.endswith("/tests/data/tree/notebooks/listfiles.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "notebooks"

    r = resources[1]
    assert r.source.endswith("/tests/data/tree/notebooks/listsecrets0.ipynb")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"

    r = resources[2]
    assert r.source.endswith("/tests/data/tree/notebooks/listsecrets1.py")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"

    r = resources[3]
    assert r.source.endswith("/tests/data/tree/notebooks/select.sql")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"
    assert r.language == "SQL"

    r = resources[4]
    assert r.source.endswith("/tests/data/tree/pyfiles/hello.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"

    r = resources[5]
    assert r.source.endswith("/tests/data/tree/pyfiles/sysversion.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"


def test_workspace_tree_rel():
    cwd = Path(os.getcwd())
    tree = get_workspace_tree()
    tree.source = os.path.relpath(tree.source, cwd)

    resources = tree.core_resources
    assert len(resources) == 6

    for r in resources:
        print(r.source)

    r = resources[0]
    assert r.source == tree.source + "/notebooks/listfiles.py"


def test_workspace_tree_with_path():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath), path="/Workspace/tmp/"
    )

    resources = tree.core_resources
    assert len(resources) == 6

    assert [r.path for r in resources] == [
        "/Workspace/tmp/notebooks/listfiles.py",
        "/Workspace/tmp/notebooks/listsecrets0.ipynb",
        "/Workspace/tmp/notebooks/listsecrets1.py",
        "/Workspace/tmp/notebooks/select.sql",
        "/Workspace/tmp/pyfiles/hello.py",
        "/Workspace/tmp/pyfiles/sysversion.py",
    ]


def test_access_controls():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath),
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )

    r0 = tree.core_resources[0]
    assert isinstance(r0, lk.models.resources.databricks.WorkspaceFile)
    r1 = tree.core_resources[1]
    assert isinstance(r1, lk.models.resources.databricks.Permissions)
    assert r1.access_control[0].permission_level == "CAN_READ"
    assert r1.workspace_file_path == "/.laktory/notebooks/listfiles.py"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_workspace_tree())
