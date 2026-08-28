from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import WorkspaceFile
from laktory.models.resources.databricks.permissions import Permissions


def get_workspace_file():
    return WorkspaceFile(
        source="./test_workspacefile.py",
        dirpath="/init_scripts/",
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )


def test_workspace_file():
    workspace_file = get_workspace_file()
    assert workspace_file.filename == "test_workspacefile.py"
    assert workspace_file.path == "/.laktory/init_scripts/test_workspacefile.py"
    assert (
        workspace_file.resource_safe_key == "laktory-init_scripts-test_workspacefile-py"
    )
    assert (
        workspace_file.resource_name
        == "workspace-file-laktory-init_scripts-test_workspacefile-py"
    )

    assert workspace_file.access_controls[0].permission_level == "CAN_READ"
    assert workspace_file.access_controls[0].group_name == "account users"


def test_workspacefile_dirpath_backslash_normalized():
    """Regression test (#616): a dirpath carrying a native Windows separator
    (leading backslash) must not be treated as an absolute anchor when
    joined with settings.workspace_root - that would silently drop the root
    instead of concatenating."""
    wf = WorkspaceFile(source="./hello_world.py", dirpath="\\scripts")
    assert wf.path == "/.laktory/scripts/hello_world.py"


def test_workspacefile_additional_resources():
    wf = get_workspace_file()
    assert len(wf.additional_core_resources) == 1
    assert isinstance(wf.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_workspace_file())
