from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Token

token = Token(
    comment="laktory",
    lifetime_seconds=3600,
)


def test_token():
    assert token.comment == "laktory"
    assert token.lifetime_seconds == 3600


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(token)
