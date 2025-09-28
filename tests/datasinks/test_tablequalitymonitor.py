import sys

import pytest

from laktory.models import UnityCatalogDataSink
from laktory.models.resources.databricks import QualityMonitor


def test_create_or_update(wsclient):
    if not sys.version.startswith("3.12"):
        # Run only for a single version of python to
        # prevent collision when writing to Unity Catalog.
        pytest.skip()

    # Config
    catalog = "laktory"
    schema = "unit_tests"
    table = "sin"

    # Snapshot
    sink = UnityCatalogDataSink(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        format="delta",
        databricks_quality_monitor=QualityMonitor(
            assets_dir="/Workspace/unit-tests/quality-monitors/",
            output_schema_name=f"{catalog}.{schema}",
            notifications={"on_failure": {"email_addresses": ["a@b.com"]}},
            snapshot={},
        ),
    )
    sdk = sink.databricks_quality_monitor.sdk(wsclient)
    sdk.delete()
    sdk.create_or_update()
    qm0 = sdk.get()

    # Update - New metric
    sink.databricks_quality_monitor.custom_metrics = [
        {
            "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
            "name": "avg_sin_cos",
            "input_columns": ["sin", "cos"],
            "definition": "avg(sin+cos)",
            "output_data_type": "double",
        },
    ]
    sdk = sink.databricks_quality_monitor.sdk(wsclient)
    sdk.create_or_update()
    qm1 = sdk.get()

    # TimeSeries
    sink.databricks_quality_monitor = QualityMonitor(
        assets_dir="/Workspace/unit-tests/quality-monitors/",
        output_schema_name=f"{catalog}.{schema}",
        time_series={
            "granularities": ["1 day"],
            "timestamp_col": "tstamp",
        },
    )
    sdk = sink.databricks_quality_monitor.sdk(wsclient)
    sdk.create_or_update()
    qm2 = sdk.get()

    assert qm0.table_name == "laktory.unit_tests.sin"
    assert qm0.monitor_version == 0
    assert qm0.snapshot is not None
    assert qm0.time_series is None
    assert qm1.table_name == "laktory.unit_tests.sin"
    assert qm1.monitor_version == 1
    assert qm1.snapshot is not None
    assert qm1.time_series is None
    assert qm2.table_name == "laktory.unit_tests.sin"
    assert qm2.monitor_version == 0
    assert qm2.snapshot is None
    assert qm2.time_series is not None
