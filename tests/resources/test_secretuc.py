from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import SecretUc

secret = SecretUc(
    catalog_name="dev", schema_name="secrets", name="api-key", value="s3cr3t"
)


def test_secretuc():
    assert secret.resource_key == "dev-secrets-api-key"
    assert secret.value == "s3cr3t"
    assert secret.terraform_resource_type == "databricks_secret_uc"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(secret)
