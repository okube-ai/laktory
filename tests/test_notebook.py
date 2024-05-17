from laktory.models.resources.databricks import Notebook

nb = Notebook(
    source="./test_notebook.py",
    path="/laktory/hello",
)


def test_notebook():
    nb0 = Notebook(source="./hello_world.py")
    nb1 = Notebook(source="./notebooks/demos/hello_world.py")
    nb2 = Notebook(source="./notebooks/demos/hello_world.py", dirpath="/files/")

    assert nb.path == "/laktory/hello"
    assert nb.source == "./test_notebook.py"
    assert nb0.path is None
    assert nb1.path == "/.laktory/demos/hello_world.py"
    assert nb2.path == "/files/hello_world.py"
    assert nb1.resource_key == "laktory-demos-hello_world-py"
    assert nb1.resource_name == "notebook-laktory-demos-hello_world-py"


if __name__ == "__main__":
    test_notebook()
