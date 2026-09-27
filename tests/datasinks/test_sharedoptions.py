import pytest
from pydantic import ValidationError

from laktory import models


def _sink(**kwargs):
    return models.UnityCatalogDataSink(
        schema_name="default", table_name="shared", mode="APPEND", **kwargs
    )


@pytest.mark.parametrize("shared", [{}, True])
def test_defaults(shared):
    sink = _sink(shared=shared)
    assert sink.shared.column == "_laktory_writer"
    assert sink.shared.where is None
    assert sink.shared.uses_writer_column


def test_disabled():
    assert _sink(shared=False).shared is None


def test_writer_id():
    def _writer_id(shared):
        sink = _sink(shared=shared)
        node = models.PipelineNode(name="node0", sinks=[sink])
        pl = models.Pipeline(name="pl", nodes=[node])
        return pl.nodes[0].sinks[0].shared.writer_id

    assert _writer_id({}) == "pl.node0"
    assert _writer_id({"writer_id": "client_a"}) == "client_a"
    assert _writer_id({"where": "client_id = 23"}) is None
    assert _sink(shared={}).shared.writer_id is None


def test_where():
    sink = _sink(shared={"where": "client_id = 23"})
    assert not sink.shared.uses_writer_column
    assert sink._shared_delete_predicate() == "(client_id = 23)"


@pytest.mark.parametrize(
    "shared,match",
    [
        ({"owner": "pipeline"}, "owner"),
        ({"where": "client_id = 23", "writer_id": "x"}, "can't be set with it"),
        ({"where": "client_id = 23", "column": "w"}, "can't be set with it"),
    ],
)
def test_invalid_options(shared, match):
    with pytest.raises(ValidationError, match=match):
        _sink(shared=shared)


def test_invalid_mode():
    with pytest.raises(ValidationError, match="only support `APPEND`"):
        models.UnityCatalogDataSink(
            schema_name="default",
            table_name="shared",
            mode="OVERWRITE",
            shared=True,
        )


def test_writer_column_requires_delta():
    with pytest.raises(ValidationError, match="require a DELTA"):
        models.FileDataSink(
            path="/tmp/shared/",
            format="PARQUET",
            mode="APPEND",
            shared=True,
        )


def test_writer_column_ignores_reset_mode():
    # Accepted (and ignored on full refresh) so that serialized configs, where inherited
    # values are explicit, can be reloaded
    _sink(shared=True, reset_mode="TRUNCATE")


def test_standalone_write_requires_writer_id():
    import polars as pl

    sink = models.FileDataSink(
        path="/tmp/shared-standalone/",
        format="DELTA",
        mode="APPEND",
        shared=True,
    )
    with pytest.raises(ValueError, match="`shared.writer_id` must be set"):
        sink.write(pl.DataFrame({"x": [1]}))
