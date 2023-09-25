import os

from laktory import Settings


def test_settings():
    settings0 = Settings()
    settings1 = Settings(landing_mount_path="/mnt")
    os.environ["LAKTORY_LANDING_MOUNT_PATH"] = "staging"
    settings2 = Settings()
    del os.environ["LAKTORY_LANDING_MOUNT_PATH"]

    assert settings0.landing_mount_path == "/mnt/landing/"
    assert settings1.landing_mount_path == "/mnt"
    assert settings2.landing_mount_path == "staging"


if __name__ == "__main__":
    test_settings()
