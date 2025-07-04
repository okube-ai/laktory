from pathlib import Path

from laktory.models.resources.databricks import PythonPackage


def get_python_package():
    dirpath = Path(__file__).parent
    config_filepath = (
        dirpath
        / "../../laktory/resources/quickstart-stacks/workflows/lake/pyproject.toml"
    )
    config_filepath = config_filepath.absolute().resolve()
    return PythonPackage(
        package_name="lake",
        config_filepath=str(config_filepath),
        dirpath="/wheels/",
    )


def test_python_package():
    pp = get_python_package()
    assert pp.source.endswith(
        "laktory/laktory/resources/quickstart-stacks/workflows/lake/dist/lake-0.0.1-py3-none-any.whl"
    )
    assert Path(pp.source).exists()
    assert pp.path == "/.laktory/wheels/lake-0.0.1-py3-none-any.whl"


def test_python_package_path():
    dirpath = Path(__file__).parent
    config_filepath = (
        dirpath
        / "../../laktory/resources/quickstart-stacks/workflows/lake/pyproject.toml"
    )
    config_filepath = config_filepath.absolute().resolve()
    pp = PythonPackage(
        package_name="lake",
        config_filepath=str(config_filepath),
        path="/Workspace/lake_1.whl",
    )
    assert pp.path == "/Workspace/lake_1.whl"
