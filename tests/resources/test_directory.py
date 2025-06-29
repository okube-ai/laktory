from laktory.models.resources.databricks import Directory

directory = Directory(path=".laktory/pipelines/")


def test_directory():
    assert directory.path == ".laktory/pipelines/"
    assert directory.resource_name == "directory-laktory-pipelines"


if __name__ == "__main__":
    test_directory()
