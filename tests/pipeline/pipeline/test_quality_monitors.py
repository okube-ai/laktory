"""Tests for Pipeline quality monitor configuration."""

import pytest

from laktory import models


def test_monitor_flag_default():
    pl = models.Pipeline(name="pl", nodes=[])
    assert pl.databricks_quality_monitor_enabled is False


def test_monitor_flag_enabled():
    pl = models.Pipeline(name="pl", databricks_quality_monitor_enabled=True, nodes=[])
    assert pl.databricks_quality_monitor_enabled is True


def test_monitor_config_model():
    node = models.PipelineNode(
        name="node_with_qm",
        source={"table_name": "laktory.unit_tests.sin"},
        sinks=[
            models.UnityCatalogDataSink(
                table_name="laktory.unit_tests.sin",
                databricks_quality_monitor=models.resources.databricks.QualityMonitor(
                    assets_dir="/tmp/",
                    output_schema_name="laktory.unit_tests",
                    snapshot={},
                ),
            )
        ],
    )
    sink = node.sinks[0]
    assert sink.databricks_quality_monitor is not None
    assert sink.databricks_quality_monitor.assets_dir == "/tmp/"


@pytest.mark.databricks_connect
def test_update_quality_monitors_live(wsclient):
    pl = models.Pipeline(
        name="pl",
        databricks_quality_monitor_enabled=True,
        nodes=[
            models.PipelineNode(
                name="node_with_qm",
                source={"table_name": "laktory.unit_tests.sin"},
                sinks=[
                    models.UnityCatalogDataSink(
                        table_name="laktory.unit_tests.sin",
                        databricks_quality_monitor=models.resources.databricks.QualityMonitor(
                            assets_dir="/tmp/",
                            output_schema_name="laktory.unit_tests",
                            snapshot={},
                        ),
                    )
                ],
            ),
        ],
    )
    pl.update_quality_monitors(workspace_client=wsclient)
