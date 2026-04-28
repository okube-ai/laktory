from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import OboToken

obo_token = OboToken(
    application_id="some_app",
)


def test_obotoken():
    assert obo_token.application_id == "some_app"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(obo_token)
