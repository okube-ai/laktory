from laktory.models.resources.databricks import QualityMonitor

qm = QualityMonitor(
    assets_dir="/.laktory/qualitymonitors",
    output_schema_name="dev.monitoring",
    table_name="dev.slv_stock_prices",
    snapshot={},
)


def test_quality_monitor():
    print(qm)
