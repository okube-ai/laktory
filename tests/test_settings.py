import os

from laktory import Settings


def test_settings():
    settings0 = Settings()
    settings1 = Settings(laktory_root="/tmp/laktory/")
    os.environ["LAKTORY_ROOT"] = "/tmp2/laktory/"
    settings2 = Settings()
    del os.environ["LAKTORY_ROOT"]

    assert settings0.laktory_root == "./"
    assert settings1.laktory_root == "/tmp/laktory/"
    assert settings2.laktory_root == "/tmp2/laktory/"


if __name__ == "__main__":
    test_settings()
