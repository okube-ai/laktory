import pytest

from laktory import get_spark_session
from laktory.models import HiveMetastoreDataSink


def _create_table(schema, table, path):
    spark = get_spark_session()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.createDataFrame(
        [(1, "acme"), (2, "acme"), (3, "other")], ["id", "client_id"]
    ).write.format("DELTA").mode("OVERWRITE").option("path", str(path)).saveAsTable(
        f"{schema}.{table}"
    )


def test_purge_drop_unchanged(tmp_path):
    schema, table = "default", "purge_drop"
    _create_table(schema, table, tmp_path / "drop")

    sink = HiveMetastoreDataSink(schema_name=schema, table_name=table)
    assert sink.purge_mode == "DROP"

    sink.purge()
    assert not sink.exists()


def test_purge_mode_override_rejects_delete_where(tmp_path):
    schema, table = "default", "purge_override_reject"
    _create_table(schema, table, tmp_path / "override_reject")

    sink = HiveMetastoreDataSink(schema_name=schema, table_name=table)
    with pytest.raises(ValueError):
        sink.purge(mode="DELETE_WHERE")


def test_purge_mode_override_drop(tmp_path):
    schema, table = "default", "purge_override_drop"
    _create_table(schema, table, tmp_path / "override_drop")

    sink = HiveMetastoreDataSink(
        schema_name=schema, table_name=table, purge_mode="TRUNCATE"
    )
    sink.purge(mode="DROP")
    assert not sink.exists()


def test_purge_truncate_table(tmp_path):
    schema, table = "default", "purge_truncate"
    _create_table(schema, table, tmp_path / "truncate")

    sink = HiveMetastoreDataSink(
        schema_name=schema, table_name=table, purge_mode="TRUNCATE"
    )
    sink.purge()

    assert sink.exists()
    spark = get_spark_session()
    assert spark.table(sink.full_name).count() == 0


def test_purge_truncate_view_raises(tmp_path):
    schema, table, view = "default", "purge_truncate_view_src", "purge_truncate_view"
    _create_table(schema, table, tmp_path / "truncate_view_src")

    spark = get_spark_session()
    spark.sql(
        f"CREATE OR REPLACE VIEW {schema}.{view} AS SELECT * FROM {schema}.{table}"
    )

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=view,
        table_type="VIEW",
        purge_mode="TRUNCATE",
    )
    with pytest.raises(ValueError):
        sink.purge()


def test_purge_delete_where(tmp_path, caplog, monkeypatch):
    import laktory.models.datasinks.tabledatasink as tds_module

    monkeypatch.setattr(tds_module.logger, "propagate", True)

    schema, table = "default", "purge_delete_where"
    _create_table(schema, table, tmp_path / "delete_where")

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        purge_mode="DELETE_WHERE",
        purge_delete_where="client_id = 'acme'",
    )
    with caplog.at_level("INFO"):
        sink.purge()

    assert sink.exists()
    spark = get_spark_session()
    rows = spark.table(sink.full_name).collect()
    assert len(rows) == 1
    assert rows[0]["client_id"] == "other"
    assert "Deleting 2 rows" in caplog.text


def test_purge_delete_where_missing_predicate():
    with pytest.raises(ValueError):
        HiveMetastoreDataSink(
            schema_name="default",
            table_name="purge_delete_where_missing",
            purge_mode="DELETE_WHERE",
        )


def test_purge_delete_where_requires_delta():
    with pytest.raises(ValueError):
        HiveMetastoreDataSink(
            schema_name="default",
            table_name="purge_delete_where_format",
            format="PARQUET",
            purge_mode="DELETE_WHERE",
            purge_delete_where="client_id = 'acme'",
        )


@pytest.mark.parametrize("mode", ["DROP", "TRUNCATE", "DELETE_WHERE"])
def test_purge_checkpoint_purged_in_all_modes(mode, tmp_path):
    schema, table = "default", f"purge_checkpoint_{mode.lower()}"
    _create_table(schema, table, tmp_path / f"checkpoint_{mode}")

    checkpoint_path = tmp_path / "checkpoint"
    checkpoint_path.mkdir()

    kwargs = {}
    if mode == "DELETE_WHERE":
        kwargs["purge_delete_where"] = "client_id = 'acme'"

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        purge_mode=mode,
        checkpoint_path_=checkpoint_path,
        **kwargs,
    )
    sink.purge()

    assert not checkpoint_path.exists()


def _shared_pipeline(name, table, path, shared, node_names):
    from laktory import models
    from laktory._testing import get_df0

    sink = {
        "schema_name": "default",
        "table_name": table,
        "mode": "APPEND",
        "format": "DELTA",
        "writer_kwargs": {"path": path},
        "shared": shared,
    }
    return models.Pipeline(
        name=name,
        dataframe_backend="PYSPARK",
        nodes=[
            models.PipelineNode(
                name=n, sources=[{"df": get_df0("PYSPARK")}], sinks=[sink]
            )
            for n in node_names
        ],
    )


def test_purge_shared_internal_table(tmp_path):
    table = "purge_shared_internal"
    pl = _shared_pipeline(
        "pl", table, (tmp_path / "t").as_posix(), {"internal": True}, ["a", "b"]
    )
    assert [t.name for t in pl.get_execution_plan().tasks] == [f"shared-{table}"]

    spark = get_spark_session()
    for _ in range(2):
        pl.execute(full_refresh=True)
        assert spark.table(f"default.{table}").count() == 6


def test_purge_shared_external_table(tmp_path):
    table = "purge_shared_external"
    path = (tmp_path / "t").as_posix()
    pl1 = _shared_pipeline("pl1", table, path, {"external": True}, ["a"])
    pl2 = _shared_pipeline("pl2", table, path, {"external": True}, ["b"])

    spark = get_spark_session()
    pl1.execute(full_refresh=True)
    pl2.execute(full_refresh=True)
    pl2.execute(full_refresh=True)
    df = spark.table(f"default.{table}")
    assert df.columns[0] == "_laktory_writer"
    rows = df.groupBy("_laktory_writer").count().collect()
    assert {r[0]: r[1] for r in rows} == {"pl1": 3, "pl2": 3}
