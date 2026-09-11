from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsCustomerManagedKeys

key = MwsCustomerManagedKeys(
    account_id="00000000-0000-0000-0000-000000000000",
    use_cases=["MANAGED_SERVICES"],
    aws_key_info={
        "key_arn": "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000",
    },
)


def test_mwscustomermanagedkeys():
    assert key.account_id == "00000000-0000-0000-0000-000000000000"
    assert key.use_cases == ["MANAGED_SERVICES"]
    assert (
        key.aws_key_info.key_arn
        == "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    )
    assert key.resource_key == "MANAGED_SERVICES"
    assert key.terraform_resource_type == "databricks_mws_customer_managed_keys"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(key)
