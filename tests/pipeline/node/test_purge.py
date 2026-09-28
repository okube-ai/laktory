"""Tests for PipelineNode.purge() and Pipeline.purge()."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from laktory import models
from laktory._testing import get_df0


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_single_sink_purge(backend, tmp_path):
    df0 = get_df0(backend)
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    sink_path = str(tmp_path / "sink") + ("/" if backend == "PYSPARK" else "")

    node = models.PipelineNode(
        name="node0",
        sources=[{"df": df0}],
        sinks=[{"path": sink_path, "format": "PARQUET", "mode": mode}],
    )
    node.execute()

    # Sink exists before purge
    assert Path(sink_path).exists()
    node.purge()
    assert not Path(sink_path).exists()


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_multi_sink_purge(backend, tmp_path):
    df0 = get_df0(backend)

    table_path = tmp_path / "df0/"
    df0.to_native().write.mode("OVERWRITE").option("path", str(table_path)).saveAsTable(
        "default.df0_purge"
    )

    node = models.PipelineNode(
        name="node0",
        sources=[{"schema_name": "default", "table_name": "df0_purge"}],
        sinks=[
            {"schema_name": "default", "table_name": "df1_purge", "table_type": "VIEW"},
            {"schema_name": "default", "table_name": "df2_purge", "table_type": "VIEW"},
        ],
        transformer={"nodes": [{"expr": "SELECT id FROM {df}"}]},
    )
    node.purge()  # should not raise even with multiple sinks


def test_checkpoint_removed(tmp_path):
    """Streaming node with expectations creates a checkpoint; purge removes it."""
    from laktory._testing import StreamingSource

    ss_path = str(tmp_path / "source")
    sink_path = str(tmp_path / "sink")
    checkpoint_path = tmp_path / "checkpoints" / "expectations"

    ss = StreamingSource("PYSPARK")
    ss.write_to_delta(ss_path)

    node = models.PipelineNode(
        name="node0",
        sources=[{"path": ss_path, "format": "DELTA", "as_stream": True}],
        expectations_checkpoint_path_=checkpoint_path,
        expectations=[
            models.DataQualityExpectation(name="warn", expr="x1 < 100", action="WARN")
        ],
        sinks=[{"path": sink_path, "format": "DELTA", "mode": "APPEND"}],
    )
    node.execute()

    # Expectations checkpoint was created
    assert checkpoint_path.exists()

    node.purge()
    assert not checkpoint_path.exists()


def test_checkpoint_removed_volumes_path(tmp_path, monkeypatch):
    """A checkpoint under a `/Volumes/{catalog}/{schema}/{volume}/...`-shaped
    path is removed by the plain-filesystem branch of the purge logic alone
    (Unity Catalog Volumes are FUSE-mounted like a regular filesystem, unlike
    legacy DBFS) - the DBFS fallback (`WorkspaceClient().dbfs.*`) must never
    be reached. See `.claude/docs/plan_a6_runtime_root_volumes.md`.
    """
    from laktory._testing import StreamingSource

    volume_root = tmp_path / "Volumes" / "main" / "default" / "laktory_vol"
    ss_path = str(volume_root / "source")
    sink_path = str(volume_root / "sink")
    checkpoint_path = volume_root / "checkpoints" / "expectations"

    ss = StreamingSource("PYSPARK")
    ss.write_to_delta(ss_path)

    node = models.PipelineNode(
        name="node0",
        sources=[{"path": ss_path, "format": "DELTA", "as_stream": True}],
        expectations_checkpoint_path_=checkpoint_path,
        expectations=[
            models.DataQualityExpectation(name="warn", expr="x1 < 100", action="WARN")
        ],
        sinks=[{"path": sink_path, "format": "DELTA", "mode": "APPEND"}],
    )
    node.execute()
    assert checkpoint_path.exists()

    mock_client = MagicMock()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: mock_client)

    node.purge()

    assert not checkpoint_path.exists()
    mock_client.dbfs.get_status.assert_not_called()
    mock_client.dbfs.delete.assert_not_called()


def _volumes_node(tmp_path):
    return models.PipelineNode(
        name="node0",
        root_path_="/Volumes/main/default/laktory_vol/node0",
        dataframe_backend="PYSPARK",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        expectations=[
            models.DataQualityExpectation(name="warn", expr="x1 < 100", action="WARN")
        ],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )


def test_checkpoint_removed_volumes_path_not_visible(tmp_path, monkeypatch):
    """Volumes checkpoints not visible on the local file system (e.g. serverless
    compute) are deleted through the Databricks SDK, which routes `/Volumes/`
    paths to the Files API. Both the sink and the expectations checkpoints are
    covered."""
    node = _volumes_node(tmp_path)
    assert not node.expectations_checkpoint_path.exists()

    mock_client = MagicMock()
    mock_client.dbfs.exists.return_value = True
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: mock_client)

    node.purge()

    deleted = [c.args[0] for c in mock_client.dbfs.delete.call_args_list]
    assert deleted == [
        f"dbfs:{node.sinks[0].checkpoint_path.as_posix()}",
        f"dbfs:{node.expectations_checkpoint_path.as_posix()}",
    ]
    for c in mock_client.dbfs.delete.call_args_list:
        assert c.kwargs == {"recursive": True}
    mock_client.dbfs.get_status.assert_not_called()


def test_checkpoint_removed_volumes_path_not_created(tmp_path, monkeypatch):
    """A Volumes checkpoint that was never created (e.g. `full_refresh` before the
    node's first run) is skipped without raising."""
    node = _volumes_node(tmp_path)

    # Checkpoints are Volumes-rooted and were never created
    assert node.expectations_checkpoint_path.as_posix().startswith("/Volumes/")
    sink_checkpoint_path = node.sinks[0].checkpoint_path
    assert sink_checkpoint_path.as_posix().startswith("/Volumes/")

    mock_client = MagicMock()
    mock_client.dbfs.exists.return_value = False
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: mock_client)

    node.purge()  # should not raise

    assert mock_client.dbfs.exists.call_count == 2
    mock_client.dbfs.get_status.assert_not_called()
    mock_client.dbfs.delete.assert_not_called()


def test_purge_never_executed(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[
            {"format": "PARQUET", "path": str(tmp_path / "sink/"), "mode": "OVERWRITE"}
        ],
    )
    node.purge()  # should not raise


def test_reset_mode_sink_override(tmp_path):
    node = models.PipelineNode(
        name="node0",
        reset_mode="TRUNCATE",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[
            {
                "format": "PARQUET",
                "path": str(tmp_path / "sink/"),
                "reset_mode": "DROP",
            }
        ],
    )
    assert node.sinks[0].reset_mode == "DROP"


def _table_and_file_sinks(tmp_path):
    return [
        {"schema_name": "default", "table_name": "reset_mode_default"},
        {"format": "CSV", "path": str(tmp_path / "sink/")},
    ]


def test_reset_mode_pipeline_level_default(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=_table_and_file_sinks(tmp_path),
    )
    models.Pipeline(name="pl", nodes=[node], reset_mode="TRUNCATE")
    # Inherited value not supported by the file sink: DROP
    assert [s.reset_mode for s in node.sinks] == ["TRUNCATE", "DROP"]
    assert node.sinks[1]._resolve_reset_mode_source() == ("TRUNCATE", "Pipeline 'pl'")


def test_reset_mode_global_settings_default(tmp_path, monkeypatch):
    from laktory._settings import settings

    monkeypatch.setattr(settings, "reset_mode", "TRUNCATE")

    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=_table_and_file_sinks(tmp_path),
    )
    assert [s.reset_mode for s in node.sinks] == ["TRUNCATE", "DROP"]
    assert node.sinks[1]._resolve_reset_mode_source() == (
        "TRUNCATE",
        "`settings.reset_mode`",
    )


def test_reset_mode_inherited_round_trip(tmp_path):
    """Job tasks reload the pipeline from its config file, where the effective sink values
    are serialized: an inherited value not supported by a sink must not become an explicit
    (rejected) one."""
    import json

    pl = models.Pipeline(
        name="pl",
        reset_mode="TRUNCATE",
        nodes=[
            {
                "name": "node0",
                "sources": [{"format": "PARQUET", "path": str(tmp_path / "src/")}],
                "sinks": _table_and_file_sinks(tmp_path),
            }
        ],
        orchestrator={"type": "LAKEFLOW_JOB", "serverless_environment_version": "3"},
    )
    content = pl.orchestrator.config_file.content_dict
    pl2 = models.Pipeline.model_validate_json(json.dumps(content))
    assert [s.reset_mode for s in pl2.nodes[0].sinks] == ["TRUNCATE", "DROP"]


def test_reset_mode_delete_where_rejected_on_node(tmp_path):
    with pytest.raises(ValueError):
        models.PipelineNode(
            name="node0",
            reset_mode="DELETE_WHERE",
            sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
            sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
        )


def test_reset_mode_delete_where_rejected_on_pipeline(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    with pytest.raises(ValueError):
        models.Pipeline(name="pl", nodes=[node], reset_mode="DELETE_WHERE")


def test_reset_mode_delete_where_rejected_globally(monkeypatch):
    from laktory._settings import settings

    with pytest.raises(ValueError):
        monkeypatch.setattr(settings, "reset_mode", "DELETE_WHERE")


@pytest.mark.parametrize("reset_mode", ["NONE", "UNKNOWN"])
def test_reset_mode_invalid_rejected_globally(reset_mode, monkeypatch):
    from laktory._settings import settings

    with pytest.raises(ValueError):
        monkeypatch.setattr(settings, "reset_mode", reset_mode)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_pipeline_purge(backend, tmp_path):
    df0 = get_df0(backend)
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz") + ("/" if backend == "PYSPARK" else "")

    node = models.PipelineNode(
        name="brz",
        sources=[{"df": df0}],
        sinks=[{"format": "PARQUET", "path": brz_path, "mode": mode}],
    )
    pl = models.Pipeline(name="pl", nodes=[node], dataframe_backend=backend)
    pl.execute()
    assert Path(brz_path).exists()

    pl.purge()
    assert not Path(brz_path).exists()


# --------------------------------------------------------------------------- #
# Shared sinks                                                                #
# --------------------------------------------------------------------------- #


def _writers(sink_path, shared, names=("a", "b", "c"), feeds=None, node_kwargs=None):
    """Independent nodes appending to the same DELTA path"""
    df0 = get_df0("POLARS")
    feeds = feeds or {}
    node_kwargs = node_kwargs or {}
    nodes = []
    for name in names:
        sink = {"path": sink_path, "format": "DELTA", "mode": "APPEND"}
        per_node = isinstance(shared, dict) and set(shared) <= set(names)
        # Nodes missing from a per-node mapping use the default options
        _shared = shared.get(name, True) if per_node else shared
        if _shared is not None:
            sink["shared"] = _shared
        feed = feeds.get(name, name)
        nodes += [
            models.PipelineNode(
                name=name,
                sources=[{"df": df0}],
                transformer={
                    "nodes": [{"expr": f"SELECT *, '{feed}' AS feed FROM {{df}}"}]
                },
                sinks=[sink],
                **node_kwargs.get(name, {}),
            )
        ]
    return nodes


def _pipeline(nodes, name="pl"):
    return models.Pipeline(name=name, nodes=nodes, dataframe_backend="POLARS")


def _read(sink_path):
    import polars as pl

    return pl.read_delta(sink_path)


def _feed_counts(sink_path):
    return dict(_read(sink_path).group_by("feed").len().sort("feed").iter_rows())


_NODE_OWNED = True


def test_shared_required(tmp_path):
    """Several nodes writing to the same target must declare `shared` on every sink"""
    path = str(tmp_path / "shared")

    with pytest.raises(
        ValueError, match=r"sinks of nodes \['a', 'b', 'c'\] don't declare"
    ):
        _pipeline(_writers(path, None))

    # Declared on some sinks only
    shared = {"a": True, "b": None, "c": None}
    with pytest.raises(ValueError, match=r"sinks of nodes \['b', 'c'\] don't declare"):
        _pipeline(_writers(path, shared))

    # A single writer doesn't need it
    _pipeline(_writers(path, None, names=("a",)))


def test_shared_node_owned_quarantine(tmp_path):
    """Nodes sending their quarantined rows to the same table keep their own tasks, even when
    they are not adjacent, and each one owns its quarantined rows."""
    df0 = get_df0("POLARS")
    quarantine = str(tmp_path / "quarantine")

    def _node(name, depends_on=None, quarantined=True):
        sinks = [{"path": str(tmp_path / name), "format": "DELTA", "mode": "OVERWRITE"}]
        expectations = []
        if quarantined:
            expectations = [
                {"name": "none", "expr": "feed = 'none'", "action": "QUARANTINE"}
            ]
            sinks += [
                {
                    "path": quarantine,
                    "format": "DELTA",
                    "mode": "APPEND",
                    "is_quarantine": True,
                    "shared": True,
                }
            ]
        return models.PipelineNode(
            name=name,
            sources=[{"df": df0}],
            depends_on=depends_on or [],
            transformer={
                "nodes": [{"expr": f"SELECT *, '{name}' AS feed FROM {{df}}"}]
            },
            expectations=expectations,
            sinks=sinks,
        )

    pl = _pipeline(
        [
            _node("a"),
            _node("x", depends_on=["a"], quarantined=False),
            _node("c", depends_on=["x"]),
        ]
    )
    plan = pl.get_execution_plan()
    assert [t.name for t in plan.tasks] == ["node-a", "node-x", "node-c"]
    assert plan.tasks_dict["node-c"].upstream_task_names == ["node-x"]

    pl.execute()
    pl.execute(refresh="full")
    pl.execute(selects=["c"], refresh="full")
    assert _feed_counts(quarantine) == {"a": 3, "c": 3}


def test_shared_node_owned_execution_task_names(tmp_path):
    # Writers keep their own execution task names
    nodes = _writers(
        str(tmp_path / "shared"),
        _NODE_OWNED,
        names=("a", "b"),
        node_kwargs={
            "a": {"execution_task_name": "t1"},
            "b": {"execution_task_name": "t2"},
        },
    )
    tasks = _pipeline(nodes).get_execution_plan().tasks
    assert [t.name for t in tasks] == ["t1", "t2"]


def test_shared_ignores_reset_mode(tmp_path):
    # Each node deletes its own rows: `reset_mode` doesn't apply and may differ
    path = str(tmp_path / "shared")
    nodes = _writers(path, _NODE_OWNED, node_kwargs={"a": {"reset_mode": "TRUNCATE"}})
    pl = _pipeline(nodes)
    pl.execute()
    pl.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_removed_node(tmp_path):
    path = str(tmp_path / "shared")
    _pipeline(_writers(path, _NODE_OWNED)).execute()

    # Node c removed: its rows are not deleted by a full refresh of the other writers
    pl = _pipeline(_writers(path, _NODE_OWNED, names=("a", "b")))
    pl.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}

    # Cleaned up by a reset of the whole table
    pl.execute(refresh="reset", reset_mode="DROP")
    pl.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3}


def test_shared_node_owned(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _NODE_OWNED))

    # One task per writer
    assert sorted(t.name for t in pl.get_execution_plan().tasks) == [
        "node-a",
        "node-b",
        "node-c",
    ]

    pl.execute()
    df = _read(path)
    assert df.columns[0] == "_laktory_writer"
    assert dict(
        df.select("feed", "_laktory_writer").unique().sort("feed").iter_rows()
    ) == {"a": "pl.a", "b": "pl.b", "c": "pl.c"}

    # Incremental run of b only appends b rows, refresh of b only replaces b rows
    pl.execute(selects=["b"])
    assert _feed_counts(path) == {"a": 3, "b": 6, "c": 3}
    pl.execute(selects=["b"], refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}

    # Any order
    for name in ["c", "a"]:
        pl.execute(selects=[name], refresh="full")
        assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_node_owned_override_rejected(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _NODE_OWNED))
    pl.execute()
    with pytest.raises(ValueError, match="Run with `refresh='RESET'`"):
        pl.execute(refresh="full", reset_mode="DROP")


def test_shared_several_pipelines(tmp_path):
    path = str(tmp_path / "shared")

    # Two pipelines writing to the same target
    pl1 = _pipeline(_writers(path, True, names=("a",)), name="pl1")
    pl2 = _pipeline(_writers(path, True, names=("b",)), name="pl2")
    pl1.execute()
    pl2.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3}
    assert sorted(_read(path)["_laktory_writer"].unique()) == ["pl1.a", "pl2.b"]

    # Full refresh of pl2 deletes its rows only
    pl2.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3}

    # A full refresh never deletes the rows of the other writers: override rejected
    with pytest.raises(ValueError, match="would also delete the rows of the other"):
        pl2.execute(refresh="full", reset_mode="DROP")
    assert _feed_counts(path) == {"a": 3, "b": 3}

    # Reset of the whole table, pl1 rows lost until pl1 is refreshed
    pl2.execute(refresh="reset", reset_mode="DROP")
    pl2.execute()
    assert _feed_counts(path) == {"b": 3}
    pl1.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3}


def test_shared_where(tmp_path):
    """Rows owned by a SQL predicate instead of a writer column"""
    path = str(tmp_path / "shared")
    shared = {n: {"where": f"feed = '{n}'"} for n in ["a", "b", "c"]}
    pl = _pipeline(_writers(path, shared))

    pl.execute()
    assert "_laktory_writer" not in _read(path).columns
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}

    # Full refresh of a writer deletes the rows matching its predicate only
    pl.execute(selects=["b"])
    assert _feed_counts(path) == {"a": 3, "b": 6, "c": 3}
    pl.execute(selects=["b"], refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_reset_mode_override_invalid(tmp_path):
    pl = _pipeline(_writers(str(tmp_path / "shared"), _NODE_OWNED))
    with pytest.raises(ValueError, match="not supported"):
        pl.execute(refresh="full", reset_mode="DELETE_WHERE")


@pytest.mark.parametrize(
    "shared,match",
    [
        ({"a": {"where": "feed = 'a'"}}, "identify their rows differently"),
        ({"a": {"column": "writer"}}, "different `shared.column`"),
        (
            {n: {"where": "feed = 'x'"} for n in ["a", "b", "c"]},
            "same `shared.where`",
        ),
        (
            {"a": {"writer_id": "x"}, "b": {"writer_id": "x"}},
            "same `shared.writer_id`",
        ),
    ],
)
def test_shared_pipeline_validation(tmp_path, shared, match):
    with pytest.raises(ValueError, match=match):
        _pipeline(_writers(str(tmp_path / "shared"), shared))


@pytest.mark.parametrize("sink", [{"mode": "OVERWRITE"}, {"format": "PARQUET"}])
def test_shared_multiple_writers_requirements(tmp_path, sink):
    """Several writers of a target require shared DELTA sinks in APPEND mode (sink-level
    validation of `shared`: tests/datasinks/test_sharedoptions.py)"""
    df0 = get_df0("POLARS")
    path = str(tmp_path / "shared")
    nodes = [
        models.PipelineNode(
            name=name,
            sources=[{"df": df0}],
            sinks=[{"path": path, "format": "DELTA", "mode": "APPEND"} | sink],
        )
        for name in ["a", "b"]
    ]
    with pytest.raises(ValueError, match="DELTA table or file sinks in `APPEND` mode"):
        _pipeline(nodes)


def test_shared_config_round_trip(tmp_path):
    """Job tasks reload the pipeline from its config file, where inherited values such as
    `reset_mode` are serialized explicitly."""
    import json

    def _node(name, path, shared):
        return {
            "name": name,
            "sources": [{"format": "JSON", "path": str(tmp_path / "src")}],
            "sinks": [
                {"path": path, "format": "DELTA", "mode": "APPEND", "shared": shared}
            ],
        }

    shared_path = str(tmp_path / "shared")
    nodes = [
        _node("a", shared_path, _NODE_OWNED),
        _node("b", shared_path, _NODE_OWNED),
        _node("c", str(tmp_path / "ext"), {"where": "feed = 'c'"}),
    ]
    pl = models.Pipeline(
        name="pl",
        nodes=nodes,
        reset_mode="TRUNCATE",
        orchestrator={"type": "LAKEFLOW_JOB", "serverless_environment_version": "3"},
    )
    content = pl.orchestrator.config_file.content_dict
    pl2 = models.Pipeline.model_validate_json(json.dumps(content))
    assert pl2.nodes_dict["a"].sinks[0].shared.writer_id == "pl.a"
    assert pl2.nodes_dict["c"].sinks[0].shared.where == "feed = 'c'"


def test_reset_single_writer(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _NODE_OWNED))
    pl.execute()

    # Each writer deletes its own rows
    pl.execute(refresh="reset", selects=["a"])
    assert _feed_counts(path) == {"b": 3, "c": 3}

    # Whole table dropped by a single writer, e.g. from a job run of one task
    pl.execute(refresh="reset", reset_mode="DROP", selects=["a"])
    assert not Path(path).exists()

    pl.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_reset_node_owned(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _NODE_OWNED))
    pl.execute()

    # Without override: each writer deletes its own rows
    pl.execute(refresh="reset", selects=["b"])
    assert _feed_counts(path) == {"a": 3, "c": 3}
    pl.execute(refresh="reset")
    assert Path(path).exists()
    assert _read(path).height == 0

    # With override: the table is dropped, e.g. before a breaking schema change
    pl.execute()
    pl.execute(refresh="reset", reset_mode="DROP")
    assert not Path(path).exists()
    pl.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_reset_several_pipelines(tmp_path):
    path = str(tmp_path / "shared")
    pl1 = _pipeline(_writers(path, True, names=("a",)), name="pl1")
    pl2 = _pipeline(_writers(path, True, names=("b",)), name="pl2")
    pl1.execute()
    pl2.execute()

    pl2.execute(refresh="reset")
    assert _feed_counts(path) == {"a": 3}

    pl2.execute(refresh="reset", reset_mode="DROP")
    assert not Path(path).exists()


def test_refresh_parameters(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _NODE_OWNED))
    pl.execute()

    with pytest.raises(ValueError, match="requires `refresh`"):
        pl.execute(reset_mode="DROP")
    with pytest.raises(ValueError, match="is not supported"):
        pl.execute(refresh="partial")
    with pytest.raises(TypeError):
        pl.execute(full_refresh=True)


def test_validate_run_parameters(tmp_path):
    pl = _pipeline(_writers(str(tmp_path / "shared"), _NODE_OWNED))

    assert pl.validate_run_parameters(None, None) == ("INCREMENTAL", None)
    # Case-insensitive
    assert pl.validate_run_parameters("full", "drop") == ("FULL", "DROP")
    assert pl.validate_run_parameters("reset", "") == ("RESET", None)
    assert pl.validate_run_parameters("reset", "DROP", node_names=["a"]) == (
        "RESET",
        "DROP",
    )
    with pytest.raises(ValueError, match="Run with `refresh='RESET'`"):
        pl.validate_run_parameters("full", "DROP", node_names=["a"])


@pytest.mark.parametrize(
    "shared",
    [
        {n: True for n in ["a", "b"]},
        {n: {"where": f"feed = '{n}'"} for n in ["a", "b"]},
    ],
)
def test_shared_node_source(tmp_path, shared):
    """A node reading a writer of a shared sink gets the writer output only, whether it's
    read from memory (same run) or from the sink (e.g. separate job task)."""
    df0 = get_df0("POLARS").to_native()
    source = str(tmp_path / "source.parquet")
    df0.write_parquet(source)
    path = str(tmp_path / "shared")

    def _pl(c_path):
        nodes = []
        for name in ["a", "b"]:
            sink = {"path": path, "format": "DELTA", "mode": "APPEND"}
            sink["shared"] = shared[name]
            nodes += [
                {
                    "name": name,
                    "sources": [{"path": source, "format": "PARQUET"}],
                    "transformer": {
                        "nodes": [{"expr": f"SELECT *, '{name}' AS feed FROM {{df}}"}]
                    },
                    "sinks": [sink],
                }
            ]
        nodes += [
            {
                "name": "c",
                "sources": [{"node_name": "a"}],
                "sinks": [{"path": c_path, "format": "DELTA", "mode": "OVERWRITE"}],
            }
        ]
        return models.Pipeline(name="pl", dataframe_backend="POLARS", nodes=nodes)

    def _c_output(pl):
        df = pl.nodes_dict["c"].output_df.collect().to_native()
        return df.columns, dict(df.group_by("feed").len().iter_rows())

    # Same run: from memory
    pl = _pl(str(tmp_path / "c1"))
    pl.execute()
    from_memory = _c_output(pl)

    # Node c alone (e.g. job task): from the shared sink
    pl = _pl(str(tmp_path / "c2"))
    pl.execute(selects=["c"])
    from_sink = _c_output(pl)

    assert from_memory == from_sink
    assert "_laktory_writer" not in from_sink[0]
    assert from_sink[1] == {"a": 3}
