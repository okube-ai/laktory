from datetime import datetime

from laktory.models import InitScript


def test_init_script():
    init_script = InitScript(
        source="../libraries/init_scripts/install_laktory.sh",
        permissions=[{"permission_level": "CAN_READ", "group_name": "account users"}],
    )
    assert init_script.filename == "install_laktory.sh"
    assert init_script.key == "init_scripts-install_laktory"
    assert init_script.path == "/init_scripts/install_laktory.sh"

    assert init_script.permissions[0].permission_level == "CAN_READ"
    assert init_script.permissions[0].group_name == "account users"


if __name__ == "__main__":
    test_init_script()
