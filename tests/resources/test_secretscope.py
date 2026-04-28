from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import SecretScope

scope = SecretScope(
    name="my-scope",
    secrets=[
        {"key": "s1", "string_value": "v1"},
        {"key": "s2", "string_value": "v2"},
    ],
)


def test_secret_scope():
    assert scope.resource_name == "secret-scope-my-scope"
    s1 = scope.additional_core_resources[0]
    s2 = scope.additional_core_resources[1]
    assert s1.resource_name == "secret-scope-my-scope-s1"
    assert s2.resource_name == "secret-scope-my-scope-s2"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(scope)
