from laktory.models.resources.databricks import SecretScope

scope = SecretScope(
    name="my-scope",
    secrets=[
        {"key": "s1", "value": "v1"},
        {"key": "s2", "value": "v2"},
    ],
)


def test_secret_scope():
    assert scope.resource_name == "secret-scope-my-scope"
    s1 = scope.additional_core_resources[0]
    s2 = scope.additional_core_resources[1]
    assert s1.resource_name == "secret-scope-my-scope-s1"
    assert s2.resource_name == "secret-scope-my-scope-s2"
