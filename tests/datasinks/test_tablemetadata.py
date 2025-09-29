import random
import string
import time

import pytest

import laktory as lk
from laktory import get_spark_session
from laktory._testing import get_df0
from laktory.models import HiveMetastoreDataSink
from laktory.models import UnityCatalogDataSink


@pytest.fixture()
def tags():
    def random_string(length=4):
        return "".join(random.choices(string.ascii_lowercase, k=length))

    return {
        "framework": "laktory",
        "random_value": random_string(4),
        random_string(4): "random_key",
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
        format="delta",
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
    sink.write(df0)

    # Update metadata
    meta0 = sink.metadata.current
    print(meta0.model_dump())
    sink.metadata.execute()

    # Read metadata
    meta1 = sink.metadata.get_current()

    # Test
    assert meta1.comment == "unit test table"
    assert meta1.properties == {
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2",
        "lk.version": "0",
        "option.mergeSchema": "false",
        "option.overwriteSchema": "true",
    }
    assert meta1.columns[0].comment is None
    assert meta1.columns[1].comment == "identification column"
    assert meta1.columns[2].comment == "x one"


@pytest.mark.databricks_connect
def test_uc_table(spark, tags):
    # Config
    catalog = "laktory"
    schema = "unit_tests"
    table = "sin"

    sink = UnityCatalogDataSink(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        metadata=lk.models.TableDataSinkMetadata(
            comment="unit test table",
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
    assert meta1.comment == "unit test table"
    assert meta1.owner == "olivier.soucy@okube.ai"
    assert meta1.properties == {
        "delta.feature.appendOnly": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.invariants": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7",
        "lk.installed": "true",
        "lk.version": "0",
    }
    assert meta1.tags == tags
    assert meta1.columns[0].comment == "Timestamp"
    assert meta1.columns[1].comment is None
    assert meta1.columns[2].comment == "sin function"
    assert meta1.columns[2].tags == tags
