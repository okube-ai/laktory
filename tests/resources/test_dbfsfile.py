from laktory.models.resources.databricks import DbfsFile

dbfs_file = DbfsFile(
    source="./test_workspacefile.py",
    dirpath="/tmp/",
    access_controls=[{"permission_level": "CAN_READ", "group_name": "account users"}],
)


def test_dbfs_file():
    print(dbfs_file)
    assert dbfs_file.filename == "test_workspacefile.py"
    assert dbfs_file.path == "/tmp/test_workspacefile.py"
    assert dbfs_file.resource_key == "tmp-test_workspacefile-py"
    print(dbfs_file.resource_name)
    assert dbfs_file.resource_name == "dbfs-file-tmp-test_workspacefile-py"

    assert dbfs_file.access_controls[0].permission_level == "CAN_READ"
    assert dbfs_file.access_controls[0].group_name == "account users"


if __name__ == "__main__":
    test_dbfs_file()
