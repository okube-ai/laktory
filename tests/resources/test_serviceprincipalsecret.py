from laktory import models
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import ServicePrincipalSecret

sp_secret = ServicePrincipalSecret(service_principal_id="1234", lifetime="7776000s")


def test_serviceprincipalsecret():
    assert sp_secret.service_principal_id == "1234"
    assert sp_secret.lifetime == "7776000s"
    assert sp_secret.resource_key == "1234"
    assert sp_secret.terraform_resource_type == "databricks_service_principal_secret"


def test_secret_reference():
    # The generated secret is an output that other resources can reference
    stack = models.Stack(
        name="test",
        organization="test",
        resources={
            "databricks_serviceprincipals": {"sp-neptune": {"display_name": "neptune"}},
            "databricks_serviceprincipalsecrets": {
                "sp-neptune-secret": {
                    "service_principal_id": "${resources.sp-neptune.id}"
                }
            },
            "databricks_secrets": {
                "secret-neptune": {
                    "scope": "laktory",
                    "key": "neptune-secret",
                    "string_value": "${resources.sp-neptune-secret.secret}",
                }
            },
            "providers": {"databricks": {"host": "https://adb.azuredatabricks.net"}},
        },
        environments={"dev": {}},
    )
    resources = stack.to_terraform(env_name="dev").model_dump()["resource"]

    sp_secret = resources["databricks_service_principal_secret"]["sp-neptune-secret"]
    assert (
        sp_secret["service_principal_id"]
        == "${databricks_service_principal.sp-neptune.id}"
    )
    secret = resources["databricks_secret"]["secret-neptune"]
    assert secret["string_value"] == (
        "${databricks_service_principal_secret.sp-neptune-secret.secret}"
    )


def test_nested_secrets():
    sp = models.resources.databricks.ServicePrincipal(
        display_name="neptune",
        secrets=[
            {"lifetime": "7776000s"},
            {},
            {"resource_options": {"name": "neptune-ci"}},
        ],
    )
    secrets = [
        r for r in sp.additional_core_resources if isinstance(r, ServicePrincipalSecret)
    ]
    assert [s.resource_name for s in secrets] == [
        f"{sp.resource_name}-secret",
        f"{sp.resource_name}-secret-1",
        "neptune-ci",
    ]
    for s in secrets:
        assert s.service_principal_id == f"${{resources.{sp.resource_name}.id}}"
    assert secrets[0].lifetime == "7776000s"
    assert "secrets" not in sp.terraform_properties


def test_nested_secret_reference():
    stack = models.Stack(
        name="test",
        organization="test",
        resources={
            "databricks_serviceprincipals": {
                "sp-neptune": {"display_name": "neptune", "secrets": [{}]}
            },
            "databricks_secrets": {
                "secret-neptune": {
                    "scope": "laktory",
                    "key": "neptune-secret",
                    "string_value": "${resources.sp-neptune-secret.secret}",
                }
            },
            "providers": {"databricks": {"host": "https://adb.azuredatabricks.net"}},
        },
        environments={"dev": {}},
    )
    resources = stack.to_terraform(env_name="dev").model_dump()["resource"]

    sp_secret = resources["databricks_service_principal_secret"]["sp-neptune-secret"]
    assert (
        sp_secret["service_principal_id"]
        == "${databricks_service_principal.sp-neptune.id}"
    )
    secret = resources["databricks_secret"]["secret-neptune"]
    assert secret["string_value"] == (
        "${databricks_service_principal_secret.sp-neptune-secret.secret}"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(sp_secret)
