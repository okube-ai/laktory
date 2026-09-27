import pytest
from pydantic import ValidationError

from laktory import models


def _sink(**kwargs):
    return models.UnityCatalogDataSink(
        schema_name="default", table_name="shared", mode="APPEND", **kwargs
    )


def test_defaults():
    sink = _sink(shared={"owner": "pipeline"})
    assert sink.shared.owner == "pipeline"
    assert sink.shared.column == "_laktory_writer"
    assert sink.shared.uses_writer_column is True


@pytest.mark.parametrize(
    "owner,uses_writer_column",
    [
        ("table", False),
        ("pipeline", True),
        ("node", True),
    ],
)
def test_uses_writer_column(owner, uses_writer_column):
    assert (
        _sink(shared={"owner": owner}).shared.uses_writer_column is uses_writer_column
    )


def test_writer_id():
    def _writer_id(shared):
        sink = _sink(shared=shared)
        node = models.PipelineNode(name="node0", sinks=[sink])
        pl = models.Pipeline(name="pl", nodes=[node])
        return pl.nodes[0].sinks[0].shared.writer_id

    assert _writer_id({"owner": "table"}) is None
    assert _writer_id({"owner": "pipeline"}) == "pl"
    assert _writer_id({"owner": "node"}) == "pl.node0"
    assert _writer_id({"owner": "pipeline", "writer_id": "client_a"}) == "client_a"
    assert _sink(shared={"owner": "pipeline"}).shared.writer_id is None


@pytest.mark.parametrize(
    "shared,match",
    [
        (True, "expects options, not a boolean"),
        ({}, "owner"),
        ({"owner": "task"}, "owner"),
        ({"isolated": True}, "owner"),
    ],
)
def test_invalid_options(shared, match):
    with pytest.raises(ValidationError, match=match):
        _sink(shared=shared)


def test_owner_table():
    # Same as no `shared` options: any mode and format
    sink = models.FileDataSink(
        path="/tmp/shared/",
        format="PARQUET",
        mode="OVERWRITE",
        shared={"owner": "table"},
    )
    assert not sink.shared.uses_writer_column


def test_invalid_mode():
    with pytest.raises(ValidationError, match="only supports `APPEND`"):
        models.UnityCatalogDataSink(
            schema_name="default",
            table_name="shared",
            mode="OVERWRITE",
            shared={"owner": "pipeline"},
        )


def test_writer_column_requires_delta():
    with pytest.raises(ValidationError, match="require a DELTA"):
        models.FileDataSink(
            path="/tmp/shared/",
            format="PARQUET",
            mode="APPEND",
            shared={"owner": "pipeline"},
        )


def test_writer_column_ignores_reset_mode():
    # Accepted (and ignored on full refresh) so that serialized configs, where inherited
    # values are explicit, can be reloaded
    _sink(shared={"owner": "pipeline"}, reset_mode="TRUNCATE")


def test_standalone_write_requires_writer_id():
    import polars as pl

    sink = models.FileDataSink(
        path="/tmp/shared-standalone/",
        format="DELTA",
        mode="APPEND",
        shared={"owner": "pipeline"},
    )
    with pytest.raises(ValueError, match="`shared.writer_id` must be set"):
        sink.write(pl.DataFrame({"x": [1]}))
