"""
Tests for Spark DataFrame / Column namespace registration via laktory.api.
"""

import pytest

import laktory as lk
from laktory._testing import get_df0

from ..conftest import assert_dfs_equal

# --------------------------------------------------------------------------- #
# Fixtures - registered once per module to avoid re-patching PySpark classes  #
# --------------------------------------------------------------------------- #


@lk.api.register_spark_dataframe_namespace("spk_test")
class _SpkTestDFOps:
    """Sample DataFrame namespace: adds x2 = x1 * 2."""

    def __init__(self, _df):
        self._df = _df

    def with_x2(self):
        import pyspark.sql.functions as F

        return self._df.withColumn("x2", F.col("x1") * 2)


@lk.api.register_spark_column_namespace("spk_col_test")
class _SpkTestColOps:
    """Sample Column namespace: doubles the column value."""

    def __init__(self, _col):
        self._col = _col

    def double(self):
        return self._col * 2


# --------------------------------------------------------------------------- #
# DataFrame namespace tests                                                    #
# --------------------------------------------------------------------------- #


def test_decorator_returns_class_unchanged():
    # Decorated class is still directly instantiable (decorator returns it unchanged)
    df = get_df0("PYSPARK").to_native()
    ns = _SpkTestDFOps(df)
    result = ns.with_x2()
    assert "x2" in result.columns


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_dataframe_namespace_native_mode(backend):
    """func_name='spk_test.with_x2' + DATAFRAME_API=NATIVE dispatches correctly."""
    import narwhals as nw

    df0 = get_df0(backend)

    node = lk.models.DataFrameMethod(
        func_name="spk_test.with_x2",
        dataframe_api="NATIVE",
    )
    result = node.execute(df0)

    expected = nw.from_native(
        get_df0(backend)
        .to_native()
        .withColumn(
            "x2",
            __import__("pyspark.sql.functions", fromlist=["col"]).col("x1") * 2,
        )
    )
    assert_dfs_equal(result, expected)


# --------------------------------------------------------------------------- #
# Column namespace tests                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_column_namespace_native_mode(backend):
    """Column namespace is accessible from eval()'d func_args strings."""
    df0 = get_df0(backend)

    node = lk.models.DataFrameMethod(
        func_name="withColumn",
        func_args=["x2", "col('x1').spk_col_test.double()"],
        dataframe_api="NATIVE",
    )
    result = node.execute(df0)

    import narwhals as nw
    import pyspark.sql.functions as F

    expected = nw.from_native(
        get_df0(backend).to_native().withColumn("x2", F.col("x1") * 2)
    )
    assert_dfs_equal(result, expected)
