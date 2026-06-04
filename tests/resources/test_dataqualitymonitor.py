from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import DataQualityMonitor

dqm = DataQualityMonitor(
    object_type="table",
    object_id="dev.finance.slv_stock_prices",
    data_profiling_config={
        "output_schema_id": "dev.monitoring",
        "snapshot": {},
    },
)


def test_data_quality_monitor():
    assert dqm.object_id == "dev.finance.slv_stock_prices"
    assert dqm.object_type == "table"
    assert dqm.data_profiling_config.output_schema_id == "dev.monitoring"
    assert dqm.data_profiling_config.snapshot is not None


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(dqm)
