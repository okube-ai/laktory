import os

import narwhals as nw
import pytest
from databricks.sdk.core import Config

from laktory import get_spark_session
from laktory._logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Databricks                                                                  #
# --------------------------------------------------------------------------- #


def get_databricks_config() -> Config:
    for profile_name in [
        "laktory-dev-sp",
        "laktory-dev-pat",
    ]:
        try:
            return Config(profile=profile_name)
        except ValueError:
            pass

    host = "https://adb-2211091707396001.1.azuredatabricks.net/"
    token = os.getenv("DATABRICKS_TOKEN")
    secret = os.getenv("AZURE_CLIENT_SECRET")

    if token:
        return Config(
            host=host,
            auth_type="pat",
            serverless_compute_id="auto",
        )

    if secret:
        return Config(
            host=host,
            auth_type="azure-client-secret",
            azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
            azure_client_id=os.getenv("AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            client_secret=secret,
            serverless_compute_id="auto",
        )

    raise ValueError(
        f"Databricks credentials could not be found. Setup profile '{profile_name}' or set DATABRICKS_TOKEN or AZURE_CLIENT_SECRET environment variables."
    )


def skip_dbks_test():
    try:
        get_databricks_config()
    except ValueError as e:
        pytest.skip(
            f"Can't instantiate Databricks Config ({e}) not supported. Skipping Test."
        )
        return True


@pytest.fixture()
def wsclient():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(config=get_databricks_config())


# --------------------------------------------------------------------------- #
# DataFrame                                                                   #
# --------------------------------------------------------------------------- #


def assert_dfs_equal(result, expected, sort=True) -> None:
    # Convert to Narwhals
    result = nw.from_native(result)
    expected = nw.from_native(expected)

    # Convert to pandas
    if isinstance(result, nw.LazyFrame):
        result = result.collect("pandas")
    result = result.to_polars()

    if isinstance(expected, nw.LazyFrame):
        expected = expected.collect("pandas")
    expected = expected.to_polars()

    # Compare columns
    assert sorted(result.columns) == sorted(expected.columns)
    columns = result.columns

    # Compare rows
    assert result.height == expected.height

    if sort:
        result = result.sort(columns)
        expected = expected.sort(columns)

    # Compare content
    for c in columns:
        r = result[c].to_list()
        e = expected[c].to_list()
        if r != e:
            print(c, r, e)
        assert r == e


# --------------------------------------------------------------------------- #
# Spark                                                                       #
# --------------------------------------------------------------------------- #


@pytest.fixture()
def spark_dbks():
    raise NotImplementedError()


@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
    except (ModuleNotFoundError, ImportError):
        logger.info("Using local spark session")
        return get_spark_session()

    logger.info("Using Databricks Connect spark session")

    return DatabricksSession.builder.sdkConfig(get_databricks_config()).getOrCreate()
