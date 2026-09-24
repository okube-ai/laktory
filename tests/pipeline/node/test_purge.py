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


def test_purge_mode_sink_override(tmp_path):
    node = models.PipelineNode(
        name="node0",
        purge_mode="TRUNCATE",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[
            {
                "format": "PARQUET",
                "path": str(tmp_path / "sink/"),
                "purge_mode": "DROP",
            }
        ],
    )
    assert node.sinks[0].purge_mode == "DROP"


def test_purge_mode_pipeline_level_default(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    models.Pipeline(name="pl", nodes=[node], purge_mode="TRUNCATE")
    assert node.sinks[0].purge_mode == "TRUNCATE"


def test_purge_mode_global_settings_default(tmp_path, monkeypatch):
    from laktory._settings import settings

    monkeypatch.setattr(settings, "purge_mode", "TRUNCATE")

    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    assert node.sinks[0].purge_mode == "TRUNCATE"


def test_purge_mode_delete_where_rejected_on_node(tmp_path):
    with pytest.raises(ValueError):
        models.PipelineNode(
            name="node0",
            purge_mode="DELETE_WHERE",
            sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
            sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
        )


def test_purge_mode_delete_where_rejected_on_pipeline(tmp_path):
    node = models.PipelineNode(
        name="node0",
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "PARQUET", "path": str(tmp_path / "sink/")}],
    )
    with pytest.raises(ValueError):
        models.Pipeline(name="pl", nodes=[node], purge_mode="DELETE_WHERE")


def test_purge_mode_delete_where_rejected_globally(monkeypatch):
    from laktory._settings import settings

    with pytest.raises(ValueError):
        monkeypatch.setattr(settings, "purge_mode", "DELETE_WHERE")


def test_purge_mode_none_rejected_globally(monkeypatch):
    from laktory._settings import settings

    with pytest.raises(ValueError):
        monkeypatch.setattr(settings, "purge_mode", "NONE")


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


def test_purge_mode_none(tmp_path):
    sink_path = tmp_path / "sink"
    node = models.PipelineNode(
        name="node0",
        root_path_=tmp_path,
        sources=[{"format": "PARQUET", "path": str(tmp_path / "src/")}],
        sinks=[{"format": "DELTA", "path": str(sink_path), "purge_mode": "NONE"}],
    )
    checkpoint_path = node.sinks[0].checkpoint_path
    sink_path.mkdir()
    checkpoint_path.mkdir(parents=True)

    node.purge()

    # Data is kept, but checkpoint is reset
    assert sink_path.exists()
    assert not checkpoint_path.exists()


def _shared_sink_pipeline(sink_path, purge_modes=None, upstreams=None):
    purge_modes = purge_modes or {}
    upstreams = upstreams or {"b": "a", "c": "b"}
    df0 = get_df0("POLARS")
    nodes = []
    for name in ["a", "b", "c"]:
        upstream = upstreams.get(name)
        nodes += [
            models.PipelineNode(
                name=name,
                depends_on=[upstream] if upstream else [],
                **({"purge_mode": purge_modes[name]} if name in purge_modes else {}),
                sources=[{"df": df0}],
                transformer={
                    "nodes": [{"expr": f"SELECT *, '{name}' AS feed FROM {{df}}"}]
                },
                sinks=[{"path": sink_path, "format": "DELTA", "mode": "APPEND"}],
            )
        ]
    return models.Pipeline(name="pl", nodes=nodes, dataframe_backend="POLARS")


def _feed_counts(pl):
    df = pl.nodes_dict["a"].primary_sink.read().to_native().collect()
    return dict(df.group_by("feed").len().sort("feed").iter_rows())


_FOLLOWERS_NONE = {"b": "NONE", "c": "NONE"}


def test_shared_sink_full_refresh(tmp_path):
    pl = _shared_sink_pipeline(str(tmp_path / "shared"), _FOLLOWERS_NONE)

    pl.execute()
    assert _feed_counts(pl) == {"a": 3, "b": 3, "c": 3}

    pl.execute(full_refresh=True)
    assert _feed_counts(pl) == {"a": 3, "b": 3, "c": 3}


def test_shared_sink_full_refresh_one_task_per_node(tmp_path):
    """Mimics a Lakeflow Job, where each node runs in its own task"""
    pl = _shared_sink_pipeline(str(tmp_path / "shared"), _FOLLOWERS_NONE)
    tasks = pl.get_execution_plan().tasks
    assert [t.node_names for t in tasks] == [["a"], ["b"], ["c"]]

    for _ in range(2):
        for task in tasks:
            pl.execute(selects=task.node_names, full_refresh=True)
        assert _feed_counts(pl) == {"a": 3, "b": 3, "c": 3}


def test_shared_sink_purge_conflict(tmp_path):
    with pytest.warns(UserWarning, match=r"\['a', 'b'\] all write to"):
        pl = _shared_sink_pipeline(str(tmp_path / "shared"), {"c": "NONE"})
    assert pl.shared_sink_purge_conflicts == {str(tmp_path / "shared"): ["a", "b"]}

    pl.execute()
    with pytest.raises(ValueError, match="purge_mode: NONE"):
        pl.execute(full_refresh=True)

    # Nothing purged
    assert _feed_counts(pl) == {"a": 3, "b": 3, "c": 3}


def test_shared_sink_no_conflict(tmp_path, recwarn):
    pl = _shared_sink_pipeline(str(tmp_path / "shared"), _FOLLOWERS_NONE)
    assert list(pl.shared_sink_groups) == [str(tmp_path / "shared")]
    assert pl.shared_sink_purge_conflicts == {}
    assert not [w for w in recwarn if "write to" in str(w.message)]


def test_shared_sink_unordered_writer(tmp_path):
    with pytest.warns(UserWarning, match=r"\['c'\] write to .* after node 'a'"):
        _shared_sink_pipeline(
            str(tmp_path / "shared"), _FOLLOWERS_NONE, upstreams={"b": "a"}
        )
