import pytest

import laktory as lk
from laktory import get_spark_session
from laktory._testing import get_df0
from laktory.models import HiveMetastoreDataSink


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_hive(backend, tmp_path):
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
            # owner="okube",  # does not work on hive metastore
            properties={
                "delta.minReaderVersion": None,
                "delta.minWriterVersion": "2",
                "lk.version": "0",
            },
            columns={
                "_idx": {
                    "comment": None,
                },
                "id": {
                    "comment": "identification column",
                },
                "x1": {
                    "comment": "x one",
                    # "tags": {"env": "dev", "pii": "false"}
                },
            },
        ),
    )
    sink.write(df0)

    # Update metadata
    sink.metadata.execute()

    # Read metadata
    desc = spark.sql("DESC TABLE EXTENDED default.df").toPandas().set_index("col_name")
    dtypes = desc["data_type"].to_dict()
    comments = desc["comment"].to_dict()

    # Test
    assert dtypes["Comment"] == "unit test table"
    # assert dtypes["Owner"] == "okube"
    assert (
        dtypes["Table Properties"]
        == "[delta.minReaderVersion=1,delta.minWriterVersion=2,lk.version=0,option.mergeSchema=false,option.overwriteSchema=true]"
    )
    assert comments["_idx"] is None
    assert comments["id"] == "identification column"
    assert comments["x1"] == "x one"
