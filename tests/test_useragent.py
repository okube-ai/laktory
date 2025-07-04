import os

from laktory._useragent import DATABRICKS_USER_AGENT
from laktory._version import VERSION


def test_user_agent():
    assert os.getenv("DATABRICKS_SDK_UPSTREAM") == DATABRICKS_USER_AGENT
    assert os.getenv("DATABRICKS_SDK_UPSTREAM_VERSION") == VERSION
