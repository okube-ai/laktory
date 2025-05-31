from __future__ import annotations

import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory.enums import DataFrameBackends
from laktory.models import UnityCatalogDataSink


@pytest.mark.xfail(reason="Requires Databricks Spark Session (for now)")
@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_read_polars(backend, tmp_path):
    df0 = get_df0(backend)

    if DataFrameBackends(backend) not in ["PYSPARK"]:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Config
    catalog = "sandbox"
    schema = "default"
    table = "df"
    full_name = f"{catalog}.{schema}.{table}"

    sink = UnityCatalogDataSink(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        mode="OVERWRITE",
    )
    sink.write(df0)

    # Read back data
    if backend == "PYSPARK":
        df = df0.sparkSession.read.table(full_name)

    # Test
    assert_dfs_equal(df, df0)


def test_full_name():
    sink = UnityCatalogDataSink(
        table_name="sandbox.default.df",
    )
    assert sink.schema_name == "default"
    assert sink.table_name == "df"
