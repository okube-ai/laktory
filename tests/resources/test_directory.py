from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Directory

directory = Directory(path=".laktory/pipelines/")


def test_directory():
    assert directory.path == ".laktory/pipelines/"
    assert directory.resource_name == "directory-laktory-pipelines"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(directory)
