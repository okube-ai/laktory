from datetime import datetime

from laktory.models import WorkspaceFile


def test_workspace_file():
    workspace_file = WorkspaceFile(
        source="../libraries/init_scripts/install_laktory.sh",
        dirpath="/init_scripts/",
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )
    print(workspace_file)
    assert workspace_file.filename == "install_laktory.sh"
    assert workspace_file.path == "/init_scripts/install_laktory.sh"
    assert workspace_file.resource_key == "init_scripts-install_laktory-sh"
    assert (
        workspace_file.resource_name == "workspace-file-init_scripts-install_laktory-sh"
    )

    assert workspace_file.access_controls[0].permission_level == "CAN_READ"
    assert workspace_file.access_controls[0].group_name == "account users"


if __name__ == "__main__":
    test_workspace_file()
