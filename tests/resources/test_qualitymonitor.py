from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import QualityMonitor

qm = QualityMonitor(
    assets_dir="/.laktory/qualitymonitors",
    output_schema_name="dev.monitoring",
    table_name="dev.slv_stock_prices",
    snapshot={},
)


def test_quality_monitor():
    assert qm.table_name == "dev.slv_stock_prices"
    assert qm.output_schema_name == "dev.monitoring"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(qm)
