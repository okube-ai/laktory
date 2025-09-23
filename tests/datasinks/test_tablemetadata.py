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
            options={
                "lk.color": ["blue", "red"],
                "lk.version": None,
            },
            properties={
                "delta.minReaderVersion": None,
                "delta.minWriterVersion": "2",
                "lk.version": "0",
            },
        ),
    )
    sink.write(df0)

    # Write metadata
    sink.metadata.execute()

    # Read metadata
    desc = spark.sql("DESC TABLE EXTENDED default.df").toPandas().set_index("col_name")
    print(desc.to_string())
    dtypes = desc["data_type"].to_dict()
    print(dtypes)

    assert dtypes["Comment"] == "unit test table"
    # assert dtypes["Owner"] == "okube"
    assert (
        dtypes["Table Properties"]
        == "[delta.minReaderVersion=1,delta.minWriterVersion=2,lk.version=0,option.mergeSchema=false,option.overwriteSchema=true]"
    )

    # df = spark.sql("DESC DETAIL default.df")
    # df.show()
    #
    #     #
    #     #
    #     # # Test
    #     # assert_dfs_equal(df, df0)
    #     #
    #     # # Test purge
    #     # sink.purge()
    #     # assert not sink.exists()
    #     raise ValueError()
    #
    #
    # # @pytest.mark.xfail(reason="Requires Databricks Spark Session (for now)")
    # @pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
    # def test_write_metadata(backend, tmp_path):
    #     df0 = get_df0(backend)
    #
    #     if backend not in ["PYSPARK"]:
    #         pytest.skip(f"Backend '{backend}' not implemented.")
    #
    #     # Config
    #     catalog = "sandbox"
    #     schema = "default"
    #     table = "df"
    #     full_name = f"{catalog}.{schema}.{table}"
    #
    #     sink = lk.models.UnityCatalogDataSink(
    #         catalog_name=catalog,
    #         schema_name=schema,
    #         table_name=table,
    #         mode="OVERWRITE",
    #         metadata=lk.models.TableDataSinkMetadata(
    #             comment="test",
    #         ),
    #     )
    #
    #     print(sink.metadata)
    #     # sink.write(df0)
    #
    #     # # Read back data
    #     # if backend == "PYSPARK":
    #     #     df = df0.sparkSession.read.table(full_name)
    #     #
    #     # # Test
    #     # assert_dfs_equal(df, df0)
    #     #
    #     # # Test purge
    #     # sink.purge()
    #     # assert not sink.exists()

    raise ValueError()
