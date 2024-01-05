import os

from laktory.models import Directory

root_dir = os.path.dirname(__file__)


def test_directory():
    d = Directory(path=".laktory/pipelines/")
    assert d.path == ".laktory/pipelines/"
    assert d.resource_name == "directory-laktory-pipelines"


if __name__ == "__main__":
    test_directory()
