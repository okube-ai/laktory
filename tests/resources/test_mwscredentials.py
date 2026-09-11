from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsCredentials

credentials = MwsCredentials(
    credentials_name="my-credentials",
    role_arn="arn:aws:iam::000000000000:role/my-cross-account-role",
)


def test_mwscredentials():
    assert credentials.credentials_name == "my-credentials"
    assert (
        credentials.role_arn == "arn:aws:iam::000000000000:role/my-cross-account-role"
    )
    assert credentials.resource_key == "my-credentials"
    assert credentials.terraform_resource_type == "databricks_mws_credentials"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(credentials)
