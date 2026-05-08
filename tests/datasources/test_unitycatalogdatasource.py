import pytest

from laktory._testing import get_df0
from laktory.models import UnityCatalogDataSource

from ..conftest import assert_dfs_equal


def test_polars_backend_rejected():
    with pytest.raises(ValueError, match="does not support the Polars backend"):
        UnityCatalogDataSource(
            catalog_name="sandbox",
            schema_name="default",
            table_name="df",
            dataframe_backend="POLARS",
        )


@pytest.mark.xfail(reason="Requires Databricks Spark Session (for now)")
def test_read(tmp_path):
    df0 = get_df0("PYSPARK").to_native()

    # Config
    catalog = "sandbox"
    schema = "default"
    table = "df"
    # full_name = f"{catalog}.{schema}.{table}"
    # path = tmp_path / "hive" / "df"

    # Write Data
    # (df0.write.mode("OVERWRITE").options(path=path.as_posix()).saveAsTable(full_name))

    # Create and read source
    source = UnityCatalogDataSource(
        catalog_name=catalog, schema_name=schema, table_name=table
    )
    df = source.read()

    # Test
    assert_dfs_equal(df, df0)


def test_full_name():
    source = UnityCatalogDataSource(
        table_name="sandbox.default.df",
    )
    assert source.schema_name == "default"
    assert source.table_name == "df"
