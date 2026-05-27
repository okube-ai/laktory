"""Tests for PipelineNode view behaviour: is_view, view_definition, sql_statements."""

from laktory import get_spark_session
from laktory import models
from laktory._testing import get_df0

from ...conftest import assert_dfs_equal


def test_is_view_false():
    node = models.PipelineNode(
        name="brz",
        sinks=[{"format": "PARQUET", "path": "/brz/"}],
    )
    assert node.is_view is False


def test_is_view_true():
    node = models.PipelineNode(
        name="slv",
        sinks=[{"schema_name": "default", "table_name": "slv", "table_type": "VIEW"}],
        transformer={"nodes": [{"expr": "SELECT id FROM {df}"}]},
    )
    assert node.is_view is True


def test_view_definition():
    node = models.PipelineNode(
        name="slv",
        sinks=[{"schema_name": "default", "table_name": "slv", "table_type": "VIEW"}],
        transformer={"nodes": [{"expr": "SELECT id FROM {df}"}]},
    )
    vd = node.view_definition
    assert vd is not None


def test_sql_statements():
    node = models.PipelineNode(
        name="slv",
        sinks=[{"schema_name": "default", "table_name": "slv", "table_type": "VIEW"}],
        transformer={"nodes": [{"expr": "SELECT id FROM {df}"}]},
    )
    stmts = node.sql_statements
    assert len(stmts) > 0
    assert any("CREATE" in s.upper() or "SELECT" in s.upper() for s in stmts)


def test_execute_view(tmp_path):
    df0 = get_df0("PYSPARK")

    # Write a table first
    (
        df0.to_native()
        .write.mode("OVERWRITE")
        .option("path", str(tmp_path / "df0/"))
        .saveAsTable("default.df0")
    )

    node = models.PipelineNode(
        name="node0",
        source={"schema_name": "default", "table_name": "df0"},
        sinks=[{"schema_name": "default", "table_name": "df1", "table_type": "VIEW"}],
        transformer={"nodes": [{"expr": "SELECT id FROM {df}"}]},
    )
    df1_output = node.execute()
    df1_read = get_spark_session().read.table("default.df1")
    assert_dfs_equal(df1_output, df1_read)
