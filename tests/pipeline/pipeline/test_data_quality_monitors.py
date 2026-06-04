"""Tests for Pipeline data quality monitor configuration."""

import sys

import pytest

from laktory import models

from ...conftest import skip_dbks_test


def test_has_databricks_data_profiling_configs_true():
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="n",
                sources=[{"table_name": "a.b.c"}],
                sinks=[
                    models.UnityCatalogDataSink(
                        table_name="a.b.c",
                        databricks_data_profiling_config={
                            "output_schema_id": "a.b",
                            "snapshot": {},
                        },
                    )
                ],
            )
        ],
    )
    assert pl.has_databricks_data_profiling_configs is True


def test_has_databricks_data_profiling_configs_false():
    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="n",
                sources=[{"table_name": "a.b.c"}],
                sinks=[models.UnityCatalogDataSink(table_name="a.b.c")],
            )
        ],
    )
    assert pl.has_databricks_data_profiling_configs is False


def test_dqm_config_on_sink():
    node = models.PipelineNode(
        name="node_with_dqm",
        sources=[{"table_name": "laktory.unit_tests.sin"}],
        sinks=[
            models.UnityCatalogDataSink(
                table_name="laktory.unit_tests.sin",
                databricks_data_profiling_config={
                    "output_schema_id": "laktory.unit_tests",
                    "snapshot": {},
                },
            )
        ],
    )
    sink = node.sinks[0]
    assert sink.databricks_data_profiling_config is not None
    assert (
        sink.databricks_data_profiling_config.output_schema_id == "laktory.unit_tests"
    )
    assert sink.databricks_data_profiling_config.snapshot is not None


def test_dqm_auto_built_from_sink():
    sink = models.UnityCatalogDataSink(
        catalog_name="laktory",
        schema_name="unit_tests",
        table_name="sin",
        databricks_data_profiling_config={
            "output_schema_id": "laktory.unit_tests",
            "snapshot": {},
        },
    )
    dqm = sink.data_quality_monitor
    assert dqm is not None
    assert dqm.object_type == "table"
    assert dqm.object_id == "laktory.unit_tests.sin"
    assert dqm.data_profiling_config.output_schema_id == "laktory.unit_tests"


def test_update_data_profiling_configs_skips_when_explicitly_unmanaged():
    """managed=False: method is a no-op even when sinks have configs."""
    pl = models.Pipeline(
        name="pl",
        databricks_data_profiling_configs_managed=False,
        nodes=[
            models.PipelineNode(
                name="n",
                sources=[{"table_name": "laktory.unit_tests.sin"}],
                sinks=[
                    models.UnityCatalogDataSink(
                        table_name="laktory.unit_tests.sin",
                        databricks_data_profiling_config={
                            "output_schema_id": "laktory.unit_tests",
                            "snapshot": {},
                        },
                    )
                ],
            )
        ],
    )
    # Should not raise or attempt any SDK calls (no workspace_client provided)
    pl.update_data_profiling_configs()


@pytest.mark.databricks_connect
def test_update_data_profiling_configs_live(wsclient):
    skip_dbks_test()

    if not sys.version.startswith("3.12"):
        pytest.skip()

    catalog = "laktory"
    schema = "unit_tests"
    table = "cos"

    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="node_with_dqm",
                sources=[],
                sinks=[
                    models.UnityCatalogDataSink(
                        table_name=f"{catalog}.{schema}.{table}",
                        databricks_data_profiling_config={
                            "output_schema_id": f"{catalog}.{schema}",
                            "assets_dir": "/Workspace/unit-tests/data-quality/",
                            "snapshot": {},
                        },
                    )
                ],
            ),
        ],
    )
    pl.update_data_profiling_configs(workspace_client=wsclient)

    # Delete data profiling
    pl.nodes[0].sinks[0].databricks_data_profiling_config = None
    pl.update_data_profiling_configs(workspace_client=wsclient)
