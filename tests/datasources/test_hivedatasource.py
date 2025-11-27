from laktory._testing import get_df0
from laktory.models import HiveMetastoreDataSource

from ..conftest import assert_dfs_equal


def test_read(tmp_path):
    df0 = get_df0("PYSPARK").to_native()

    # Config
    schema = "default"
    table = "df"
    full_name = f"{schema}.{table}"
    path = tmp_path / "hive" / "df"

    # Write Data
    (df0.write.mode("OVERWRITE").options(path=path.as_posix()).saveAsTable(full_name))

    # Create and read source
    source = HiveMetastoreDataSource(schema_name=schema, table_name=table)
    df = source.read()

    # Test
    assert_dfs_equal(df, df0)


def test_full_name():
    source = HiveMetastoreDataSource(
        table_name="default.df",
    )
    assert source.schema_name == "default"
    assert source.table_name == "df"


def test_inject_vars():
    source = HiveMetastoreDataSource(catalog_name="c", schema_name="s", table_name="t")
    assert source.full_name == "c.s.t"

    source = HiveMetastoreDataSource(
        catalog_name="c",
        schema_name="s",
        table_name="${vars.t_name}",
        variables={"t_name": "t"},
    )
    source = source.inject_vars()
    assert source.full_name == "c.s.t"

    source = HiveMetastoreDataSource(
        catalog_name="c",
        schema_name="s",
        table_name="${{ vars.t_name['x'] }}",
        variables={"t_name": {"x": "t"}},
    )
    source = source.inject_vars()
    assert source.full_name == "c.s.t"
