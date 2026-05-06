from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import SecretScope
from laktory.models.resources.databricks.secretacl import SecretAcl

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


def test_permissions():
    s = SecretScope(
        name="my-scope",
        permissions=[
            {"permission": "READ", "principal": "role-admins"},
            {"permission": "WRITE", "principal": "role-engineers"},
        ],
    )
    resources = s.additional_core_resources
    assert len(resources) == 2
    assert all(isinstance(r, SecretAcl) for r in resources)
    assert resources[0].resource_options.name == "secret-scope-acl-my-scope-role-admins"
    assert (
        resources[1].resource_options.name == "secret-scope-acl-my-scope-role-engineers"
    )
    assert resources[0].resource_options.depends_on == [
        "${resources.secret-scope-my-scope}"
    ]
    assert resources[1].resource_options.depends_on == [
        "${resources.secret-scope-my-scope}"
    ]


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(scope)
