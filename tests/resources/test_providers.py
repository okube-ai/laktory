from laktory import models
from laktory.models.resources import DatabricksProvider


def test_databricks_provider():
    p = DatabricksProvider(host="databricks.net")
    assert p.host == "databricks.net"


def test_workspace_client_kwargs_resolves_vars():
    stack = models.Stack(
        name="test",
        resources={
            "providers": {
                "databricks": {
                    "host": "${vars.databricks_host}",
                    "token": "${vars.databricks_token}",
                }
            }
        },
        environments={
            "dev": {
                "variables": {
                    "databricks_host": "https://adb-xxx.azuredatabricks.net",
                    "databricks_token": "my-token",
                }
            }
        },
    )

    env = stack.get_env(env_name="dev").inject_vars()
    db_provider = next(
        (
            _r
            for r in env.resources._get_all(providers_only=True).values()
            for _r in r.core_resources
            if isinstance(_r, DatabricksProvider)
        ),
        None,
    )
    kwargs = db_provider.workspace_client_kwargs()
    assert kwargs["host"] == "https://adb-xxx.azuredatabricks.net"
    assert kwargs["token"] == "my-token"
