import pandas as pd
import pytest

from laktory import get_spark_session
from laktory._testing import assert_dfs_equal
from laktory.enums import DataFrameBackends
from laktory.models import UnityCatalogDataSink


@pytest.fixture
def df0():
    spark = get_spark_session()

    return spark.createDataFrame(
        pd.DataFrame(
            {
                "x": ["a", "b", "c"],
                "y": [3, 4, 5],
            }
        )
    )


@pytest.mark.xfail(reason="Requires Databricks Spark Session (for now)")
def test_read_polars(df0, tmp_path, backend):
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
