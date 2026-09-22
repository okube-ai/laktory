import os

import pytest

from laktory import Settings


def test_settings():
    settings0 = Settings()
    settings1 = Settings(runtime_root="/tmp/laktory/")
    os.environ["LAKTORY_RUNTIME_ROOT"] = "/tmp2/laktory/"
    settings2 = Settings()
    del os.environ["LAKTORY_RUNTIME_ROOT"]

    assert settings0.runtime_root == "./.laktory/"
    assert settings1.runtime_root == "/tmp/laktory/"
    assert settings2.runtime_root == "/tmp2/laktory/"


def test_settings_purge_mode_delete_where_rejected():
    with pytest.raises(ValueError):
        Settings(purge_mode="DELETE_WHERE")

    settings = Settings()
    with pytest.raises(ValueError):
        settings.purge_mode = "DELETE_WHERE"
