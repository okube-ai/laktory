from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Notebook
from laktory.models.resources.databricks.permissions import Permissions


def get_notebook():
    return Notebook(
        source="./test_notebook.py",
        path="/laktory/hello",
    )


def test_notebook():
    nb0 = Notebook(source="./hello_world.py")
    nb1 = Notebook(source="./hello_world.py", dirpath="notebooks/")

    nb = get_notebook()
    assert nb.path == "/laktory/hello"
    assert nb.source == "./test_notebook.py"
    assert nb0.path == "/.laktory/hello_world.py"
    assert nb1.path == "/.laktory/notebooks/hello_world.py"


def test_notebook_dirpath_backslash_normalized():
    """Regression test (#616): a dirpath carrying a native Windows separator
    (leading backslash) must not be treated as an absolute anchor when
    joined with settings.workspace_root - that would silently drop the root
    instead of concatenating."""
    nb = Notebook(source="./hello_world.py", dirpath="\\scripts")
    assert nb.path == "/.laktory/scripts/hello_world.py"


def test_notebook_laktory_root(monkeypatch):
    monkeypatch.setattr("laktory.settings.workspace_root", "/src/")
    nb3 = Notebook(source="./hello_world.py")
    assert nb3.path == "/src/hello_world.py"


def test_notebook_additional_resources():
    nb = Notebook(
        source="./hello_world.py",
        access_controls=[{"group_name": "users", "permission_level": "CAN_READ"}],
    )
    assert len(nb.additional_core_resources) == 1
    assert isinstance(nb.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_notebook())
