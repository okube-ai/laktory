import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.models import HiveMetastoreDataSink


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_write(backend, tmp_path):
    df0 = get_df0(backend)

    if backend not in ["PYSPARK"]:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Config
    schema = "default"
    table = "df"

    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
        format="parquet",  # TODO: Review why delta format can't be read
        writer_kwargs={"path": (tmp_path).as_posix()},
    )
    sink.write(df0)

    # Read back data
    df = sink.read()

    # Test
    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_create_view(backend, tmp_path):
    df0 = get_df0(backend).to_native()

    if backend not in ["PYSPARK"]:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Create table
    schema = "default"
    table = "df"
    view = "df_view"
    (
        df0.write.format("parquet")
        .mode("OVERWRITE")
        .options(mergeSchema=False, overwriteSchema=True, path=tmp_path.as_posix())
        .saveAsTable(f"{schema}.{table}")
    )

    # Create View
    sink = HiveMetastoreDataSink(
        schema_name=schema,
        table_name=view,
        view_definition=f"SELECT * FROM {schema}.{table}",
    )
    sink.write()

    # Read back data
    df = sink.read()

    # Test
    assert_dfs_equal(df, df0)


def test_full_name():
    sink = HiveMetastoreDataSink(
        table_name="default.df",
    )
    assert sink.schema_name == "default"
    assert sink.table_name == "df"
