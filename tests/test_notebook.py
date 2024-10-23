import os
from laktory import settings
from laktory.models.resources.databricks import Notebook

nb = Notebook(
    source="./test_notebook.py",
    path="/laktory/hello",
)


def test_notebook():
    nb0 = Notebook(source="./hello_world.py")
    nb1 = Notebook(source="./hello_world.py", dirpath="notebooks/")
    nb2 = Notebook(source="./hello_world.py", rootpath="/scr/notebooks")
    os.environ["LAKTORY_WORKSPACE_LAKTORY_ROOT"] = "/src/"
    settings.__init__()
    nb3 = Notebook(source="./hello_world.py")
    del os.environ["LAKTORY_WORKSPACE_LAKTORY_ROOT"]
    settings.__init__()

    assert nb.path == "/laktory/hello"
    assert nb.source == "./test_notebook.py"
    assert nb0.path == "/.laktory/hello_world.py"
    assert nb1.path == "/.laktory/notebooks/hello_world.py"
    assert nb2.path == "/scr/notebooks/hello_world.py"
    assert nb3.path == "/src/hello_world.py"


if __name__ == "__main__":
    test_notebook()
