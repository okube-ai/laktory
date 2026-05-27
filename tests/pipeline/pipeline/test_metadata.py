"""Tests for sink metadata model."""

import pytest

from laktory import models


def _node_with_metadata():
    return models.PipelineNode(
        name="gld",
        sinks=[
            {
                "schema_name": "default",
                "table_name": "gld",
                "type": "HIVE_METASTORE",
                "mode": "OVERWRITE",
                "format": "PARQUET",
                "writer_kwargs": {"path": "/gld_sink/"},
                "metadata": {
                    "comment": "Gold",
                    "columns": [{"name": "id", "comment": "Identification column"}],
                },
            }
        ],
    )


def test_metadata_model():
    from laktory.models import HiveMetastoreDataSink

    node = _node_with_metadata()
    sink = node.sinks[0]
    assert isinstance(sink, HiveMetastoreDataSink)
    assert sink.metadata.comment == "Gold"
    assert len(sink.metadata.columns) == 1
    assert sink.metadata.columns[0].name == "id"
    assert sink.metadata.columns[0].comment == "Identification column"


@pytest.mark.databricks_connect
def test_update_metadata_live(spark):
    node = _node_with_metadata()
    pl = models.Pipeline(name="pl", nodes=[node])
    pl.update_tables_metadata()
