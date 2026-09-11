from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsLogDelivery

delivery = MwsLogDelivery(
    account_id="00000000-0000-0000-0000-000000000000",
    config_name="my-audit-log-delivery",
    credentials_id="cred-id",
    log_type="AUDIT_LOGS",
    output_format="JSON",
    storage_configuration_id="storage-id",
)


def test_mwslogdelivery():
    assert delivery.account_id == "00000000-0000-0000-0000-000000000000"
    assert delivery.config_name == "my-audit-log-delivery"
    assert delivery.credentials_id == "cred-id"
    assert delivery.log_type == "AUDIT_LOGS"
    assert delivery.output_format == "JSON"
    assert delivery.storage_configuration_id == "storage-id"
    assert delivery.resource_key == "my-audit-log-delivery"
    assert delivery.terraform_resource_type == "databricks_mws_log_delivery"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(delivery)
