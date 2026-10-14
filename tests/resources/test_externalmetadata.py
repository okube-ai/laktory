from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import ExternalMetadata

metadata = ExternalMetadata(
    name="laktory_stock_prices_report",
    system_type="POWER_BI",
    entity_type="Report",
    url="https://app.powerbi.com/reports/stock-prices",
)


def test_externalmetadata():
    assert metadata.resource_name == "external-metadata-laktory_stock_prices_report"
    assert metadata.terraform_resource_type == "databricks_external_metadata"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(metadata)
