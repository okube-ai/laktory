# import importlib
# import os
# import sys
#
# import numpy as np

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql import types as T
import polars as pl
import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory._testing import get_df1
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameDataSource
from laktory.models import DataFrameMethod

# from laktory import models
# from laktory.exceptions import MissingColumnError
# from laktory.exceptions import MissingColumnsError


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

    def select2(df, *cols):
        return df.select(cols)

    node = DataFrameMethod(
        func_name="select",
        func_args=["id"],
    )
    df = node.execute(df0)
    assert df.columns == ["id"]


#
# def atest_exceptions():
#     # TODO: Re-enable when coalesce is ready
#     return
#
#     df = df0.select(df0.columns)
#
#     # Input missing - missing not allowed
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "column": {"name": "cos_x", "type": "double"},
#                 "func_name": "cos",
#                 "func_args": ["col('y')"],
#                 "allow_missing_column_args": False,
#             },
#         ]
#     )
#     with pytest.raises(MissingColumnError):
#         df = sc.execute(df)
#
#     # Input missing - missing allowed
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "column": {"name": "xy", "type": "double"},
#                 "func_name": "coalesce",
#                 "func_args": ["col('x')", "col('y')"],
#                 "allow_missing_column_args": True,
#             },
#         ]
#     )
#     df = sc.execute(df)
#     assert "xy" in df.columns
#
#     # All inputs missing
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "column": {"name": "xy", "type": "double"},
#                 "func_name": "coalesce",
#                 "func_args": ["col('z')", "col('y')"],
#                 "allow_missing_column_args": True,
#             },
#         ]
#     )
#     with pytest.raises(MissingColumnsError):
#         df = sc.execute(df)
#
#
