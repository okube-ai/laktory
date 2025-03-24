import pandas as pd
import pytest

from laktory import get_spark_session
from laktory._testing import assert_dfs_equal
from laktory.models import HiveMetastoreDataSource


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


def test_read(df0, tmp_path):
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
