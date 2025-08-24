import laktory as lk
from laktory._testing import Paths

paths = Paths(__file__)


def get_workspace_tree():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    return lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath),
    )


def test_workspace_tree():
    tree = get_workspace_tree()

    resources = tree.core_resources
    assert len(resources) == 5

    for r in resources:
        print(r.source)

    r = resources[0]
    assert r.source.endswith("/tree/pyfiles/hello.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"

    r = resources[1]
    assert r.source.endswith("/tree/pyfiles/sysversion.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"

    r = resources[2]
    assert r.source.endswith("/tree/notebooks/listsecrets.ipynb")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"

    r = resources[3]
    assert r.source.endswith("/tree/notebooks/listfiles.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "notebooks"

    r = resources[4]
    assert r.source.endswith("/tree/notebooks/listsecrets.py")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"


def test_workspace_tree_with_path():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath), path="/Workspace/tmp/"
    )

    resources = tree.core_resources
    assert len(resources) == 5

    assert [r.path for r in resources] == [
        "/Workspace/tmp/pyfiles/hello.py",
        "/Workspace/tmp/pyfiles/sysversion.py",
        "/Workspace/tmp/notebooks/listsecrets.ipynb",
        "/Workspace/tmp/notebooks/listfiles.py",
        "/Workspace/tmp/notebooks/listsecrets.py",
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
    assert r1.access_controls[0].permission_level == "CAN_READ"
    assert r1.workspace_file_path == "/.laktory/pyfiles/hello.py"
