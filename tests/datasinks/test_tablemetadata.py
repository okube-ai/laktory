import random
import string
import time
from unittest.mock import MagicMock

import pytest

import laktory as lk
from laktory import get_spark_session
from laktory._testing import get_df0
from laktory.models import HiveMetastoreDataSink
from laktory.models import UnityCatalogDataSink
from laktory.models.datasinks.tabledatasinkmetadata import set_tags


@pytest.fixture()
def tags():
    def random_string(length=4):
        return "".join(random.choices(string.ascii_lowercase, k=length))

    return {
        "framework": "laktory",
        "random_value": random_string(4),
        random_string(4): "random_key",
        "empty tag": None,
    }


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_hive_table(backend, tmp_path):
    df0 = get_df0(backend)
    spark = get_spark_session()

    if backend not in ["PYSPARK"]:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Config
    schema = "default"
    table = "df"

    # TODO: Review why this conf setting is required to overwrite delta
    spark.conf.set("spark.sql.sources.useV1SourceList", "delta")
    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        format="DELTA",
        writer_kwargs={"path": tmp_path.as_posix()},
        metadata=lk.models.TableDataSinkMetadata(
            comment="unit test table",
            properties={
                "delta.minWriterVersion": "2",
                "lk.version": "0",
            },
            columns=[
                {
                    "name": "id",
                    "comment": "identification column",
                },
                {
                    "name": "x1",
                    "comment": "x one",
                },
            ],
        ),
    )
    sink.create(df0)

    # Update metadata and write
    sink.write(df0)
    sink.metadata.execute()

    # Read metadata
    meta1 = sink.metadata.get_current()

    # Test
    assert meta1.comment == "unit test table"
    assert meta1.properties == {
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2",
        "lk.version": "0",
        "laktory.managedProperties": "delta.minWriterVersion|lk.version",
        "option.mergeSchema": "false",
        "option.overwriteSchema": "true",
    }
    assert meta1.columns[0].comment is None
    assert meta1.columns[1].comment == "identification column"
    assert meta1.columns[2].comment == "x one"

    # Remove table properties
    sink.metadata.properties = {}
    sink.metadata.execute()
    meta2 = sink.metadata.get_current()
    print(meta2.properties)
    assert meta2.properties == {
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2",
        "option.mergeSchema": "false",
        "option.overwriteSchema": "true",
    }


def test_set_tags_value_quoting(monkeypatch):
    mock_spark = MagicMock()
    monkeypatch.setattr("laktory.get_spark_session", lambda: mock_spark)

    set_tags(
        object="TABLE",
        full_name="catalog.schema.table",
        alter_target="catalog.schema.table",
        current={},
        new={"my_tag": "my_value", "quoted": "o'brien", "unset_tag": None},
        is_uc=True,
    )

    executed = [c.args[0] for c in mock_spark.sql.call_args_list]

    # Non-null values are quoted as string literals via ALTER TABLE ... SET TAGS,
    # not backtick-quoted identifiers via the unsupported "SET TAG ON ..." form
    assert (
        "ALTER TABLE catalog.schema.table SET TAGS ('my_tag' = 'my_value')" in executed
    )
    # Single quotes in the value are escaped
    assert (
        "ALTER TABLE catalog.schema.table SET TAGS ('quoted' = 'o\\'brien')" in executed
    )
    # Null values keep the key-only form
    assert "ALTER TABLE catalog.schema.table SET TAGS ('unset_tag')" in executed


@pytest.mark.databricks_connect
def test_uc_table(spark, tags):
    catalog = "laktory"
    schema = "unit_tests"
    table = "sin"

    sink = UnityCatalogDataSink(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        metadata=lk.models.TableDataSinkMetadata(
            comment="""Okube's "unit test" table""",
            owner="olivier.soucy@okube.ai",
            tags=tags,
            properties={
                "lk.version": "0",
                "lk.installed": "true",
            },
            columns=[
                {
                    "name": "tstamp",
                    "comment": "Timestamp",
                },
                {
                    "name": "sin",
                    "comment": "sin function",
                    "tags": tags,
                },
            ],
        ),
    )

    # Update metadata
    meta0 = sink.metadata.current
    print(meta0.model_dump())
    sink.metadata.execute()

    # Read metadata
    time.sleep(5.0)
    meta1 = sink.metadata.get_current()

    # Test
    assert meta1.comment == """Okube's "unit test" table"""
    assert meta1.owner == "olivier.soucy@okube.ai"
    assert meta1.properties == {
        "delta.feature.appendOnly": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.invariants": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7",
        "lk.installed": "true",
        "lk.version": "0",
        "laktory.managedProperties": "lk.installed|lk.version",
    }
    assert meta1.tags == tags
    assert meta1.columns[0].comment == "Timestamp"
    assert meta1.columns[1].comment is None
    assert meta1.columns[2].comment == "sin function"
    assert meta1.columns[2].tags == tags
