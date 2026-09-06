"""Tests for Pipeline and PipelineNode path computation."""

from pathlib import Path

from laktory import models

_SINK = {"format": "JSON", "mode": "OVERWRITE", "path": "file.json"}


def _get_pl(tmp_path):
    return models.Pipeline(
        name="pl",
        root_path=str(tmp_path),
        nodes=[
            models.PipelineNode(name="brz", sinks=[_SINK]),
            models.PipelineNode(
                name="slv", sources=[{"node_name": "brz"}], sinks=[_SINK]
            ),
        ],
    )


def test_root_path_default():
    pl = models.Pipeline(name="pl", nodes=[])
    assert pl.root_path == Path(".laktory/pipelines/pl")


def test_root_path_override(tmp_path):
    pl = models.Pipeline(name="pl", root_path=str(tmp_path), nodes=[])
    assert pl.root_path == tmp_path


def test_root_path_roundtrip():
    pl = models.Pipeline(name="pl", nodes=[])
    dump = pl.model_dump(exclude_unset=True)
    pl2 = models.Pipeline.model_validate(dump)
    assert pl2.root_path == pl.root_path


def test_node_root_path_fallback(tmp_path):
    pl = _get_pl(tmp_path)
    for node in pl.nodes:
        assert node.root_path == tmp_path / node.name


def test_node_checkpoint_path(tmp_path):
    pl = _get_pl(tmp_path)
    for node in pl.nodes:
        assert node.expectations_checkpoint_path == (
            tmp_path / node.name / "checkpoints" / "expectations"
        )


def test_sink_checkpoint_path(tmp_path):
    pl = _get_pl(tmp_path)
    for node in pl.nodes:
        for sink in node.all_sinks:
            assert sink.checkpoint_path == (
                tmp_path / node.name / "checkpoints" / f"sink-{sink._uuid}"
            )


def test_sink_checkpoint_path_after_inject_vars(tmp_path):
    # `Pipeline.inject_vars(inplace=False)` (the default) deep-copies the
    # tree. Sinks are two levels deep (pipeline -> node -> sink), which used
    # to leave `_parent` pointing at a stale, un-injected node after the
    # copy, so `checkpoint_path` silently kept the raw `${vars.env}` - see
    # issue #653.
    pl = models.Pipeline(
        name="pl",
        root_path=str(tmp_path) + "_${vars.env}",
        nodes=[models.PipelineNode(name="brz", sinks=[_SINK])],
    )

    pl2 = pl.inject_vars(vars={"env": "labs"})
    node2 = pl2.nodes[0]
    sink2 = node2.sinks[0]

    assert "${vars.env}" not in str(sink2.checkpoint_path)
    assert sink2.checkpoint_path == (
        Path(f"{tmp_path}_labs") / "brz" / "checkpoints" / f"sink-{sink2._uuid}"
    )
