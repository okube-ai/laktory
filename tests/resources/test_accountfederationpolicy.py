from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AccountFederationPolicy

policy = AccountFederationPolicy(
    policy_id="my-policy",
    description="my federation policy",
    oidc_policy={
        "issuer": "https://myidp.example.com",
        "audiences": ["api://AzureADTokenExchange"],
    },
)


def test_accountfederationpolicy():
    assert policy.policy_id == "my-policy"
    assert policy.description == "my federation policy"
    assert policy.oidc_policy.issuer == "https://myidp.example.com"
    assert policy.oidc_policy.audiences == ["api://AzureADTokenExchange"]
    assert policy.resource_key == "my-policy"
    assert policy.terraform_resource_type == "databricks_account_federation_policy"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(policy)
