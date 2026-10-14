from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import CustomAppIntegration

integration = CustomAppIntegration(
    name="laktory-unit-test",
    confidential=True,
    redirect_urls=["https://example.com/callback"],
    scopes=["all-apis"],
    token_access_policy={"access_token_ttl_in_minutes": 60},
)


def test_customappintegration():
    assert integration.resource_key == "laktory-unit-test"
    assert integration.token_access_policy.access_token_ttl_in_minutes == 60
    assert integration.terraform_resource_type == "databricks_custom_app_integration"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(integration)
