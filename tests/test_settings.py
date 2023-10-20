import os

from laktory import Settings


def test_settings():
    settings0 = Settings()
    settings1 = Settings(workspace_landing_root="/mnt/landing/")
    os.environ["LAKTORY_WORKSPACE_LANDING_ROOT"] = "staging"
    settings2 = Settings()
    del os.environ["LAKTORY_WORKSPACE_LANDING_ROOT"]

    assert settings0.workspace_landing_root == "/Volumes/dev/sources/landing/"
    assert settings1.workspace_landing_root == "/mnt/landing/"
    assert settings2.workspace_landing_root == "staging"


if __name__ == "__main__":
    test_settings()
