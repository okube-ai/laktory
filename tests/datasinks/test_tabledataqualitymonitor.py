import sys

import pytest

from laktory.models import UnityCatalogDataSink

from ..conftest import skip_dbks_test


@pytest.mark.databricks_connect
def test_create_or_update(wsclient):
    skip_dbks_test()

    if not sys.version.startswith("3.12"):
        pytest.skip()

    catalog = "laktory"
    schema = "unit_tests"
    table = "sin"

    # Snapshot
    sink = UnityCatalogDataSink(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        format="DELTA",
        databricks_data_profiling_config={
            "output_schema_id": f"{catalog}.{schema}",
            "assets_dir": "/Workspace/unit-tests/data-quality/",
            "notification_settings": {"on_failure": {"email_addresses": ["a@b.com"]}},
            "snapshot": {},
            "schedule": {
                "quartz_cron_expression": "0 0 0 * * ?",
                "timezone_id": "UTC",
            },
        },
    )
    sdk = sink.data_quality_monitor.sdk(wsclient)
    sdk.delete()
    sdk.create_or_update()
    dqm0 = sdk.get()

    # Update - Add custom metric
    sink.databricks_data_profiling_config.custom_metrics = [
        {
            "type": "DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE",
            "name": "avg_sin_cos",
            "input_columns": ["sin", "cos"],
            "definition": "avg(sin+cos)",
            "output_data_type": "double",
        },
    ]
    sdk = sink.data_quality_monitor.sdk(wsclient)
    sdk.create_or_update()
    dqm1 = sdk.get()

    # TimeSeries
    sink.databricks_data_profiling_config = type(sink.databricks_data_profiling_config)(
        output_schema_id=f"{catalog}.{schema}",
        assets_dir="/Workspace/unit-tests/data-quality/",
        time_series={
            "granularities": ["1 day"],
            "timestamp_column": "tstamp",
        },
    )
    sdk = sink.data_quality_monitor.sdk(wsclient)
    sdk.create_or_update()
    dqm2 = sdk.get()

    assert dqm0.data_profiling_config.snapshot is not None
    assert dqm0.data_profiling_config.time_series is None
    assert dqm1.data_profiling_config.snapshot is not None
    assert dqm1.data_profiling_config.time_series is None
    assert dqm2.data_profiling_config.snapshot is None
    assert dqm2.data_profiling_config.time_series is not None
