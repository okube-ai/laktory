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
            models.PipelineNode(name="slv", source={"node_name": "brz"}, sinks=[_SINK]),
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
