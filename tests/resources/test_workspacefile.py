from laktory.models.resources.databricks import WorkspaceFile


def get_workspace_file():
    return WorkspaceFile(
        source="./test_workspacefile.py",
        rootpath="/",
        dirpath="/init_scripts/",
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )


def test_workspace_file():
    workspace_file = get_workspace_file()
    assert workspace_file.filename == "test_workspacefile.py"
    assert workspace_file.path == "/init_scripts/test_workspacefile.py"
    assert workspace_file.resource_key == "init_scripts-test_workspacefile-py"
    assert (
        workspace_file.resource_name
        == "workspace-file-init_scripts-test_workspacefile-py"
    )

    assert workspace_file.access_controls[0].permission_level == "CAN_READ"
    assert workspace_file.access_controls[0].group_name == "account users"
