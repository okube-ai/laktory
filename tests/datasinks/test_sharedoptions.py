import pytest
from pydantic import ValidationError

from laktory import models


def _sink(**kwargs):
    return models.UnityCatalogDataSink(
        schema_name="default", table_name="shared", mode="APPEND", **kwargs
    )


def test_defaults():
    sink = _sink(shared={"internal": True})
    assert sink.shared.external is False
    assert sink.shared.isolated is False
    assert sink.shared.column == "_laktory_writer"
    assert sink.shared.uses_writer_column is False


@pytest.mark.parametrize(
    "shared,uses_writer_column",
    [
        ({"internal": True}, False),
        ({"internal": True, "isolated": True}, True),
        ({"external": True}, True),
        ({"internal": True, "external": True}, True),
    ],
)
def test_uses_writer_column(shared, uses_writer_column):
    assert _sink(shared=shared).shared.uses_writer_column is uses_writer_column


def test_writer_id():
    def _writer_id(shared):
        sink = _sink(shared=shared)
        node = models.PipelineNode(name="node0", sinks=[sink])
        pl = models.Pipeline(name="pl", nodes=[node])
        return pl.nodes[0].sinks[0].shared.writer_id

    assert _writer_id({"external": True}) == "pl"
    assert _writer_id({"external": True, "writer_id": "client_a"}) == "client_a"
    assert _sink(shared={"external": True}).shared.writer_id is None


@pytest.mark.parametrize(
    "shared,match",
    [
        (True, "expects options, not a boolean"),
        ({}, "requires at least one"),
    ],
)
def test_invalid_options(shared, match):
    with pytest.raises(ValidationError, match=match):
        _sink(shared=shared)


def test_isolated_without_internal():
    assert _sink(shared={"isolated": True}).shared.uses_writer_column


def test_invalid_mode():
    with pytest.raises(ValidationError, match="only support `APPEND`"):
        models.UnityCatalogDataSink(
            schema_name="default",
            table_name="shared",
            mode="OVERWRITE",
            shared={"internal": True},
        )


def test_writer_column_requires_delta():
    # Grouped internal sinks don't need a writer column: any format
    models.FileDataSink(
        path="/tmp/shared/", format="PARQUET", mode="APPEND", shared={"internal": True}
    )

    with pytest.raises(ValidationError, match="require a DELTA"):
        models.FileDataSink(
            path="/tmp/shared/",
            format="PARQUET",
            mode="APPEND",
            shared={"external": True},
        )


def test_writer_column_ignores_reset_mode():
    # Accepted (and ignored on full refresh) so that serialized configs, where inherited
    # values are explicit, can be reloaded
    _sink(shared={"internal": True}, reset_mode="TRUNCATE")
    _sink(shared={"external": True}, reset_mode="TRUNCATE")


def test_standalone_write_requires_writer_id():
    import polars as pl

    sink = models.FileDataSink(
        path="/tmp/shared-standalone/",
        format="DELTA",
        mode="APPEND",
        shared={"external": True},
    )
    with pytest.raises(ValueError, match="`shared.writer_id` must be set"):
        sink.write(pl.DataFrame({"x": [1]}))
