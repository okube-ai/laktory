from .conftest import get_databricks_config
from .conftest import skip_dbks_test


def test_databricks_config():
    skip_dbks_test()
    assert get_databricks_config() is not None


def test_workspace_client(wsclient):
    skip_dbks_test()
    assert wsclient is not None
