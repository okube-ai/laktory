from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import DbfsFile
from laktory.models.resources.databricks.permissions import Permissions

dbfs_file = DbfsFile(
    source="./test_workspacefile.py",
    dirpath="/tmp/",
    access_controls=[{"permission_level": "CAN_READ", "group_name": "account users"}],
)


def test_dbfs_file():
    assert dbfs_file.filename == "test_workspacefile.py"
    assert dbfs_file.path == "/tmp/test_workspacefile.py"
    assert dbfs_file.resource_safe_key == "tmp-test_workspacefile-py"
    assert dbfs_file.resource_name == "dbfs-file-tmp-test_workspacefile-py"

    assert dbfs_file.access_controls[0].permission_level == "CAN_READ"
    assert dbfs_file.access_controls[0].group_name == "account users"


def test_dbfsfile_additional_resources():
    assert len(dbfs_file.additional_core_resources) == 1
    assert isinstance(dbfs_file.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(dbfs_file)


def test_dbfs_file_dump_does_not_mutate():
    for dirpath in [None, "/tmp/"]:
        kwargs = {} if dirpath is None else {"dirpath": dirpath}
        f = DbfsFile(source="./test_workspacefile.py", **kwargs)
        dump0 = f.model_dump()
        assert f.dirpath == dirpath
        assert f.model_dump() == dump0
