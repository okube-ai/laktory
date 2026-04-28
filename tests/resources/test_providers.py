from laktory.models.resources import DatabricksProvider


def test_databricks_provider():
    p = DatabricksProvider(host="databricks.net")
    assert p.host == "databricks.net"
