import os

from laktory.models.resources import DatabricksProvider


def test_databricks_provider():
    p = DatabricksProvider(host="databricks.net")
    print(p)

    print(p.resource_key)
    print(p.resource_name)


if __name__ == "__main__":
    test_databricks_provider()
