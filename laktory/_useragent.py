import os

from laktory._version import VERSION

DATABRICKS_USER_AGENT = "okube-laktory"


def set_databricks_sdk_upstream():
    # Inject user-agent value for monitoring usage as a Databricks partner
    os.environ["DATABRICKS_SDK_UPSTREAM"] = DATABRICKS_USER_AGENT
    os.environ["DATABRICKS_SDK_UPSTREAM_VERSION"] = VERSION
