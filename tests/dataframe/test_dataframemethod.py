import polars as pl
import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory._testing import get_df1
from laktory.api import register_anyframe_namespace
from laktory.api import register_expr_namespace
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameDataSource
from laktory.models import DataFrameMethod


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_arg_string(backend):
    df0 = get_df0(backend)

    node = DataFrameMethod(
        func_name="select",
        func_args=["id"],
    )
    df = node.execute(df0)
    assert df.columns == ["id"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_kwarg_string(backend):
    df0 = get_df0(backend)

    node = DataFrameMethod(
        func_name="with_columns",
        func_kwargs={
            "y1": "x1",
        },
    )
    df = node.execute(df0)
    assert df.columns == ["_idx", "id", "x1", "y1"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_arg_source(backend):
    df0 = get_df0(backend)
    source = DataFrameDataSource(df=get_df1(backend))

    node = DataFrameMethod(
        func_name="join",
        func_args=[source],
        func_kwargs={
            "on": "id",
            "how": "left",
        },
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))
    assert node.data_sources == [source]


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_arg_source_native(backend):
    df0 = get_df0(backend)
    source = DataFrameDataSource(df=get_df1(backend))

    node = DataFrameMethod(
        func_name="union",
        func_args=[source],
        dataframe_api="NATIVE",
    )
    df = node.execute(df0)
    assert_dfs_equal(df, df0.to_native().union(source.read().to_native()))


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_arg_nw_expr(backend):
    df0 = get_df0(backend)

    node = DataFrameMethod(
        func_name="with_columns",
        func_kwargs={"y1": "nw.col('x1').clip(lower_bound=0, upper_bound=2)"},
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_arg_native_expr(backend):
    df0 = get_df0(backend)
    backend = DataFrameBackends(backend)

    if backend == DataFrameBackends.POLARS:
        kwargs = {
            "func_name": "with_columns",
            "func_kwargs": {"y1": "pl.col('x1').clip(lower_bound=0, upper_bound=2)"},
        }
    elif backend == DataFrameBackends.PYSPARK:
        kwargs = {
            "func_name": "withColumn",
            "func_args": ["y1", "F.greatest(F.least('x1', F.lit(2)), F.lit(0))"],
        }

    node = DataFrameMethod(dataframe_api="NATIVE", **kwargs)
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_arg_sql_expr(backend):
    df0 = get_df0(backend)

    node = DataFrameMethod(
        func_name="with_columns",
        func_kwargs={
            "y1": "sql_expr('5 * x1')",
            "y2": "nw.sql_expr('5 * x1')",
        },
    )

    df = node.execute(df0)
    assert_dfs_equal(
        df.select("y1", "y2"), pl.DataFrame({"y1": [5, 10, 15], "y2": [5, 10, 15]})
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_udf(backend):
    df0 = get_df0(backend)

    @register_expr_namespace("c0")
    class C0:
        def __init__(self, _expr):
            self._expr = _expr

        def mult2(self):
            return (self._expr * 2).alias("m2")

    @register_anyframe_namespace("c1")
    class C1:
        def __init__(self, _df):
            self._df = _df

        def f1(self, cols):
            return self._df.select(cols)

    @register_anyframe_namespace("c2")
    class C2:
        def __init__(self, _df):
            self._df = _df

        def f2(self):
            return self._df.select("x1")

    # Select with arguments
    node = DataFrameMethod(
        func_name="c1.f1",
        func_args=[["id", "x1"]],
    )
    df = node.execute(df0)
    assert df.columns == ["id", "x1"]

    # Select without arguments
    node = DataFrameMethod(
        func_name="c2.f2",
    )
    df = node.execute(df0)
    assert df.columns == ["x1"]

    # Select with custom expression
    node = DataFrameMethod(
        func_name="c1.f1",
        func_args=["nw.col('x1').c0.mult2()"],
    )
    df = node.execute(df0)
    assert df.columns == ["m2"]
