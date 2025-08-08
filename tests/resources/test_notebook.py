from laktory.models.resources.databricks import Notebook


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


def test_notebook_laktory_root(monkeypatch):
    monkeypatch.setattr("laktory.settings.workspace_laktory_root", "/src/")
    nb3 = Notebook(source="./hello_world.py")
    assert nb3.path == "/src/hello_world.py"
