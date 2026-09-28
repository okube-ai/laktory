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
    assert sink.reset_mode == "DROP"

    sink.purge()
    assert not sink.exists()


def test_reset_mode_override_rejects_invalid(tmp_path):
    schema, table = "default", "purge_override_reject"
    _create_table(schema, table, tmp_path / "override_reject")

    sink = HiveMetastoreDataSink(schema_name=schema, table_name=table)
    with pytest.raises(ValueError):
        sink.purge(mode="DELETE_WHERE")


def test_reset_mode_override_drop(tmp_path):
    schema, table = "default", "purge_override_drop"
    _create_table(schema, table, tmp_path / "override_drop")

    sink = HiveMetastoreDataSink(
        schema_name=schema, table_name=table, reset_mode="TRUNCATE"
    )
    sink.purge(mode="DROP")
    assert not sink.exists()


def test_purge_truncate_table(tmp_path):
    schema, table = "default", "purge_truncate"
    _create_table(schema, table, tmp_path / "truncate")

    sink = HiveMetastoreDataSink(
        schema_name=schema, table_name=table, reset_mode="TRUNCATE"
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

    # Set on the sink: rejected
    with pytest.raises(ValueError, match="not supported by view"):
        HiveMetastoreDataSink(
            schema_name=schema,
            table_name=view,
            table_type="VIEW",
            reset_mode="TRUNCATE",
        )

    # Override: falls back to DROP
    sink = HiveMetastoreDataSink(schema_name=schema, table_name=view, table_type="VIEW")
    sink.purge(mode="TRUNCATE")
    assert not sink.exists()


def test_reset_shared_where(tmp_path, caplog, monkeypatch):
    """A single writer owning part of a table: a reset only deletes the rows matching its
    predicate"""
    import laktory.models.datasinks.basedatasink as bds_module

    monkeypatch.setattr(bds_module.logger, "propagate", True)

    schema, table = "default", "reset_shared_where"
    _create_table(schema, table, tmp_path / "shared_where")

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        mode="APPEND",
        shared={"where": "client_id = 'acme'"},
    )
    with caplog.at_level("INFO"):
        sink.purge()

    assert sink.exists()
    spark = get_spark_session()
    rows = spark.table(sink.full_name).collect()
    assert len(rows) == 1
    assert rows[0]["client_id"] == "other"
    assert "Deleted 2 rows" in caplog.text


def test_reset_shared_where_requires_delta():
    with pytest.raises(ValueError, match="require a DELTA"):
        HiveMetastoreDataSink(
            schema_name="default",
            table_name="reset_shared_where_format",
            format="PARQUET",
            mode="APPEND",
            shared={"where": "client_id = 'acme'"},
        )


@pytest.mark.parametrize("mode", ["DROP", "TRUNCATE"])
def test_purge_checkpoint_purged_in_all_modes(mode, tmp_path):
    schema, table = "default", f"purge_checkpoint_{mode.lower()}"
    _create_table(schema, table, tmp_path / f"checkpoint_{mode}")

    checkpoint_path = tmp_path / "checkpoint"
    checkpoint_path.mkdir()

    kwargs = {}

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        reset_mode=mode,
        checkpoint_path_=checkpoint_path,
        **kwargs,
    )
    sink.purge()

    assert not checkpoint_path.exists()


def _shared_pipeline(name, table, path, shared, node_names, sink_kwargs=None):
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
                name=n,
                sources=[{"df": get_df0("PYSPARK")}],
                transformer={
                    "nodes": [{"expr": f"SELECT *, '{n}' AS feed FROM {{df}}"}]
                },
                sinks=[sink | (sink_kwargs or {}).get(n, {})],
            )
            for n in node_names
        ],
    )


def _writer_counts(table):
    rows = (
        get_spark_session()
        .table(f"default.{table}")
        .groupBy("_laktory_writer", "feed")
        .count()
        .collect()
    )
    return {(r[0], r[1]): r[2] for r in rows}


def test_purge_shared_multiple_writers_table(tmp_path):
    table = "purge_shared_writers"
    get_spark_session().sql(f"DROP TABLE IF EXISTS default.{table}")
    pl = _shared_pipeline("pl", table, (tmp_path / "t").as_posix(), True, ["a", "b"])

    # Node-owned by default, one task per writer
    assert [t.name for t in pl.get_execution_plan().tasks] == ["node-a", "node-b"]
    assert [n.sinks[0].shared.writer_id for n in pl.nodes] == ["pl.a", "pl.b"]

    for _ in range(2):
        pl.execute(refresh="full")
        assert _writer_counts(table) == {("pl.a", "a"): 3, ("pl.b", "b"): 3}

    # Full refresh of one writer: its own rows only
    pl.execute(selects=["b"])
    pl.execute(selects=["a"], refresh="full")
    assert _writer_counts(table) == {("pl.a", "a"): 3, ("pl.b", "b"): 6}


def test_purge_shared_legacy_table(tmp_path):
    """A table written before being shared has no writer column: clear error until it is
    dropped once."""
    table = "purge_shared_legacy"
    spark = get_spark_session()
    spark.sql(f"DROP TABLE IF EXISTS default.{table}")
    path = (tmp_path / "t").as_posix()

    _shared_pipeline("pl", table, path, None, ["a"]).execute()
    assert "_laktory_writer" not in spark.table(f"default.{table}").columns

    pl = _shared_pipeline("pl", table, path, True, ["a", "b"])
    for refresh in ["incremental", "full"]:
        with pytest.raises(ValueError, match="has no `_laktory_writer` column"):
            pl.execute(refresh=refresh)

    pl.execute(refresh="reset", reset_mode="DROP")
    assert not spark.catalog.tableExists(f"default.{table}")
    pl.execute()
    assert _writer_counts(table) == {("pl.a", "a"): 3, ("pl.b", "b"): 3}


def test_purge_shared_streaming_dropped_table(tmp_path):
    """A table dropped by a reset run of one of its writers only: the checkpoints of the other
    writers are reset too, so that they reprocess their source instead of resuming."""
    from laktory import models
    from laktory._testing import StreamingSource

    table = "purge_shared_stream"
    spark = get_spark_session()
    spark.sql(f"DROP TABLE IF EXISTS default.{table}")
    source = (tmp_path / "source").as_posix()
    StreamingSource("PYSPARK").write_to_delta(source)

    def _node(name):
        return {
            "name": name,
            "sources": [{"path": source, "format": "DELTA", "as_stream": True}],
            "transformer": {
                "nodes": [{"expr": f"SELECT *, '{name}' AS feed FROM {{df}}"}]
            },
            "sinks": [
                {
                    "schema_name": "default",
                    "table_name": table,
                    "mode": "APPEND",
                    "writer_kwargs": {"path": (tmp_path / "t").as_posix()},
                    "shared": True,
                }
            ],
        }

    pl = models.Pipeline(
        name="pl",
        root_path=str(tmp_path / "pl"),
        dataframe_backend="PYSPARK",
        nodes=[_node("a"), _node("b")],
    )
    expected = {("pl.a", "a"): 3, ("pl.b", "b"): 3}

    pl.execute()
    assert _writer_counts(table) == expected

    # Incremental run: nothing new to process
    pl.execute()
    assert _writer_counts(table) == expected

    # Table dropped by a reset run of `a` only, which also resets the checkpoint of `b`
    pl.execute(selects=["a"], refresh="reset", reset_mode="DROP")
    assert not spark.catalog.tableExists(f"default.{table}")

    pl.execute()
    assert _writer_counts(table) == expected


def test_purge_shared_several_pipelines_table(tmp_path):
    table = "purge_shared_pipeline"
    path = (tmp_path / "t").as_posix()
    pl1 = _shared_pipeline("pl1", table, path, True, ["a"])
    pl2 = _shared_pipeline("pl2", table, path, True, ["b"])

    spark = get_spark_session()
    pl1.execute(refresh="full")
    pl2.execute(refresh="full")
    pl2.execute(refresh="full")
    df = spark.table(f"default.{table}")
    assert df.columns[0] == "_laktory_writer"
    rows = df.groupBy("_laktory_writer").count().collect()
    assert {r[0]: r[1] for r in rows} == {"pl1.a": 3, "pl2.b": 3}


def test_purge_shared_where_table(tmp_path):
    table = "purge_shared_where"
    get_spark_session().sql(f"DROP TABLE IF EXISTS default.{table}")
    path = (tmp_path / "t").as_posix()
    pl1 = _shared_pipeline("pl1", table, path, {"where": "feed = 'a'"}, ["a"])
    pl2 = _shared_pipeline("pl2", table, path, {"where": "feed = 'b'"}, ["b"])

    spark = get_spark_session()
    pl1.execute(refresh="full")
    pl2.execute(refresh="full")
    pl2.execute(refresh="full")
    df = spark.table(f"default.{table}")
    assert "_laktory_writer" not in df.columns
    rows = df.groupBy("feed").count().collect()
    assert {r[0]: r[1] for r in rows} == {"a": 3, "b": 3}


@pytest.mark.parametrize(
    "where",
    [
        None,
        "feed = '{n}'",
        # Predicates beyond comparisons: Spark SQL
        "feed IN ('{n}', 'x')",
        "feed LIKE '{n}%'",
        "feed BETWEEN '{n}' AND '{n}'",
    ],
)
def test_shared_node_source_spark(tmp_path, where):
    """A node reading a writer of a shared table from the table gets the writer rows only"""
    from laktory import models

    table = "purge_shared_node_source"
    get_spark_session().sql(f"DROP TABLE IF EXISTS default.{table}")
    shared = True
    sink_kwargs = None
    if where:
        shared = None
        sink_kwargs = {n: {"shared": {"where": where.format(n=n)}} for n in ["a", "b"]}
    pl = _shared_pipeline(
        "pl", table, (tmp_path / "t").as_posix(), shared, ["a", "b"], sink_kwargs
    )
    pl.execute()

    source = models.PipelineNodeDataSource(node_name="a")
    source._parent = pl.nodes_dict["b"]
    pl.nodes_dict["a"]._output_df = None  # e.g. separate job task
    df = source.read().to_native()
    assert "_laktory_writer" not in df.columns
    assert {r[0] for r in df.select("feed").distinct().collect()} == {"a"}
