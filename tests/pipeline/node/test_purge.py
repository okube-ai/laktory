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


def test_checkpoint_removed_volumes_path_not_created(tmp_path, monkeypatch):
    """A `/Volumes/{catalog}/{schema}/{volume}/...`-shaped checkpoint path that
    was never created (e.g. `full_refresh` before the node's first run) must not
    be routed through the legacy DBFS API - `dbfs.get_status` on a Volumes path
    raises `PermissionDenied`, not `ResourceDoesNotExist`, so it can't be
    special-cased there. The purge must recognize the `/Volumes/` prefix and
    skip the DBFS fallback outright. This covers both the node's expectations
    checkpoint (`PipelineNode.purge()`) and a sink's own default checkpoint
    (`BaseDataSink._purge_checkpoint()`), which both derive from `root_path`
    when `runtime_root` is configured as a Databricks Volume.

    The checkpoint path is rooted at the filesystem root (not under `tmp_path`)
    so it genuinely matches the `/Volumes/` prefix, the way it would on an
    actual Databricks runtime; on this test machine it simply doesn't exist.
    """
    node = models.PipelineNode(
        name="node0",
        root_path_="/Volumes/main/default/laktory_vol/node0",
        dataframe_backend="PYSPARK",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        expectations=[
            models.DataQualityExpectation(name="warn", expr="x1 < 100", action="WARN")
        ],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )

    # Checkpoints are Volumes-rooted and were never created
    assert node.expectations_checkpoint_path.as_posix().startswith("/Volumes/")
    assert not node.expectations_checkpoint_path.exists()
    sink_checkpoint_path = node.sinks[0].checkpoint_path
    assert sink_checkpoint_path.as_posix().startswith("/Volumes/")
    assert not sink_checkpoint_path.exists()

    mock_client = MagicMock()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: mock_client)

    node.purge()  # should not raise

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


def test_reset_mode_pipeline_level_default(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    models.Pipeline(name="pl", nodes=[node], reset_mode="TRUNCATE")
    assert node.sinks[0].reset_mode == "TRUNCATE"


def test_reset_mode_global_settings_default(tmp_path, monkeypatch):
    from laktory._settings import settings

    monkeypatch.setattr(settings, "reset_mode", "TRUNCATE")

    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    assert node.sinks[0].reset_mode == "TRUNCATE"


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
        _shared = shared.get(name) if per_node else shared
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


_INTERNAL = {"internal": True}
_ISOLATED = {"internal": True, "isolated": True}


def test_shared_internal_grouped(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _INTERNAL))

    # Writers grouped in a single task, no writer column
    tasks = pl.get_execution_plan().tasks
    assert [(t.name, sorted(t.node_names)) for t in tasks] == [
        ("shared-shared", ["a", "b", "c"])
    ]

    pl.execute()
    assert "_laktory_writer" not in _read(path).columns
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}

    # Table reset once, all writers reprocess
    pl.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_internal_order(tmp_path):
    path = str(tmp_path / "shared")
    nodes = _writers(path, _INTERNAL, node_kwargs={"a": {"depends_on": ["c"]}})
    pl = _pipeline(nodes)
    assert pl.get_execution_plan().tasks[0].node_names[-1] == "a"


def test_shared_internal_selection(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _INTERNAL))
    pl.execute()

    # Selecting one writer selects the whole group: table reset, no rows lost
    assert sorted(pl.get_execution_plan(selects=["b"]).node_names) == ["a", "b", "c"]
    pl.execute(selects=["b"], refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_internal_removed_node(tmp_path):
    path = str(tmp_path / "shared")
    _pipeline(_writers(path, _INTERNAL)).execute()

    # Node c removed and pipeline "redeployed": full refresh leaves no leftovers
    pl = _pipeline(_writers(path, _INTERNAL, names=("a", "b")))
    pl.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3}


def test_shared_isolated(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _ISOLATED))

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


def test_shared_isolated_override_rejected(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _ISOLATED))
    pl.execute()
    with pytest.raises(ValueError, match="Run with `refresh='reset'`"):
        pl.execute(refresh="full", reset_mode="DROP")


def test_shared_external(tmp_path):
    path = str(tmp_path / "shared")
    external = {"external": True}

    # Two pipelines writing to the same target
    pl1 = _pipeline(_writers(path, external, names=("a",)), name="pl1")
    pl2 = _pipeline(
        _writers(path, {"internal": True, "external": True}, names=("b", "c")),
        name="pl2",
    )
    pl1.execute()
    pl2.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}
    assert sorted(_read(path)["_laktory_writer"].unique()) == ["pl1", "pl2"]

    # Full refresh of pl2 deletes its rows once, pl1 rows untouched
    pl2.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}

    # Override drops the whole table, pl1 rows lost until pl1 is refreshed
    pl2.execute(refresh="full", reset_mode="DROP")
    assert _feed_counts(path) == {"b": 3, "c": 3}
    pl1.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_reset_mode_override_invalid(tmp_path):
    pl = _pipeline(_writers(str(tmp_path / "shared"), _INTERNAL))
    with pytest.raises(ValueError, match="not supported"):
        pl.execute(refresh="full", reset_mode="DELETE_WHERE")


@pytest.mark.parametrize(
    "shared,match",
    [
        ({"a": _ISOLATED, "b": _ISOLATED}, "different `shared` options"),
        (
            {"a": _INTERNAL, "b": _INTERNAL, "c": _ISOLATED},
            "different `shared` options",
        ),
        (
            {
                "a": {**_ISOLATED, "writer_id": "x"},
                "b": {**_ISOLATED, "writer_id": "x"},
                "c": _ISOLATED,
            },
            "duplicate `shared.writer_id`",
        ),
    ],
)
def test_shared_pipeline_validation(tmp_path, shared, match):
    with pytest.raises(ValueError, match=match):
        _pipeline(_writers(str(tmp_path / "shared"), shared))


def test_shared_inferred(tmp_path):
    """Writers of a same target are grouped without any `shared` declaration"""
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, None))
    tasks = pl.get_execution_plan().tasks
    assert [(t.name, sorted(t.node_names)) for t in tasks] == [
        ("shared-shared", ["a", "b", "c"])
    ]

    pl.execute()
    pl.execute(refresh="full")
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_shared_internal_single_writer(tmp_path):
    """`internal` is documentation only: a single writer is a regular task"""
    pl = _pipeline(_writers(str(tmp_path / "shared"), _INTERNAL, names=("a",)))
    assert [t.name for t in pl.get_execution_plan().tasks] == ["node-a"]


def test_shared_grouped_task_name_conflict(tmp_path):
    nodes = _writers(
        str(tmp_path / "shared"),
        _INTERNAL,
        names=("a", "b"),
        node_kwargs={
            "a": {"execution_task_name": "t1"},
            "b": {"execution_task_name": "t2"},
        },
    )
    with pytest.raises(ValueError, match="different `execution_task_name`"):
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
        _node("a", shared_path, _ISOLATED),
        _node("b", shared_path, _ISOLATED),
        _node("c", str(tmp_path / "ext"), {"external": True}),
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
    assert pl2.nodes_dict["c"].sinks[0].shared.writer_id == "pl"


def test_reset_grouped(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, None))
    pl.execute()

    pl.execute(refresh="reset")
    assert not Path(path).exists()

    pl.execute()
    assert _feed_counts(path) == {"a": 3, "b": 3, "c": 3}


def test_reset_isolated(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, _ISOLATED))
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


def test_reset_external(tmp_path):
    path = str(tmp_path / "shared")
    pl1 = _pipeline(_writers(path, {"external": True}, names=("a",)), name="pl1")
    pl2 = _pipeline(_writers(path, {"external": True}, names=("b",)), name="pl2")
    pl1.execute()
    pl2.execute()

    pl2.execute(refresh="reset")
    assert _feed_counts(path) == {"a": 3}

    pl2.execute(refresh="reset", reset_mode="DROP")
    assert not Path(path).exists()


def test_refresh_parameters(tmp_path):
    path = str(tmp_path / "shared")
    pl = _pipeline(_writers(path, None))
    pl.execute()

    with pytest.raises(ValueError, match="requires `refresh`"):
        pl.execute(reset_mode="DROP")
    with pytest.raises(ValueError, match="is not supported"):
        pl.execute(refresh="partial")
    with pytest.raises(TypeError):
        pl.execute(full_refresh=True)


def test_validate_run_parameters(tmp_path):
    pl = _pipeline(_writers(str(tmp_path / "shared"), _ISOLATED))

    assert pl.validate_run_parameters(None, None) == ("incremental", None)
    assert pl.validate_run_parameters("FULL", "drop") == ("full", "DROP")
    assert pl.validate_run_parameters("reset", "") == ("reset", None)
    assert pl.validate_run_parameters("reset", "DROP", node_names=["a"]) == (
        "reset",
        "DROP",
    )
    with pytest.raises(ValueError, match="Run with `refresh='reset'`"):
        pl.validate_run_parameters("full", "DROP", node_names=["a"])
