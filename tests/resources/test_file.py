from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import File

file = File(
    source="./test_catalog.py",
    dirpath="/Volumes/laktory/unit_tests/landing/",
)


def test_file():
    assert file.filename == "test_catalog.py"
    assert file.path == "/Volumes/laktory/unit_tests/landing/test_catalog.py"
    assert file.resource_name == (
        "file-Volumes-laktory-unit_tests-landing-test_catalog-py"
    )
    assert file.terraform_properties == {
        "path": "/Volumes/laktory/unit_tests/landing/test_catalog.py",
        "source": "./test_catalog.py",
    }

    f = File(source="./a.json", path="/Volumes/laktory/unit_tests/files/b.json")
    assert f.path == "/Volumes/laktory/unit_tests/files/b.json"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(file)
