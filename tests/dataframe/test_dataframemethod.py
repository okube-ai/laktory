# import importlib
# import os
# import sys
#
# import numpy as np
import narwhals as nw
import pandas as pd

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql import types as T
import polars as pl
import pytest

import laktory
from laktory._testing import assert_dfs_equal
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameMethod

# from laktory import models
# from laktory.exceptions import MissingColumnError
# from laktory.exceptions import MissingColumnsError


def get_backend(v):
    if isinstance(v, str):
        return DataFrameBackends(v)
    return DataFrameBackends.from_nw_implementation(nw.from_native(v).implementation)


def to_backend(df, backend):
    backend = get_backend(backend)

    if backend == DataFrameBackends.POLARS:
        df = pl.from_pandas(df)
    elif backend == DataFrameBackends.PYSPARK:
        spark = laktory.get_spark_session()
        df = spark.createDataFrame(df)
    return nw.from_native(df)


@pytest.fixture(params=["POLARS", "PYSPARK"])
def df0(request):
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    return to_backend(df, request.param)


@pytest.fixture
def source():
    return pd.DataFrame(
        {
            "id": ["b", "c", "d"],
            "x2": [4, 9, 16],
        }
    )


def test_arg_string(df0):
    node = DataFrameMethod(
        name="select",
        args=["id"],
    )
    df = node.execute(df0)
    assert df.columns == ["id"]


def test_kwarg_string(df0):
    node = DataFrameMethod(
        name="with_columns",
        kwargs={
            "y1": "x1",
        },
    )
    df = node.execute(df0)
    assert df.columns == ["id", "x1", "y1"]


def test_arg_source(df0, source):
    source = to_backend(source, df0)

    node = DataFrameMethod(
        name="join",
        args=[source],
        kwargs={
            "on": "id",
            "how": "left",
        },
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))


def test_arg_nw_expr(df0):
    node = DataFrameMethod(
        name="with_columns",
        kwargs={"y1": "nw.col('x1').clip(lower_bound=0, upper_bound=2)"},
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


def test_arg_native_expr(df0):
    backend = get_backend(df0)

    if backend == DataFrameBackends.POLARS:
        kwargs = {
            "name": "with_columns",
            "kwargs": {"y1": "pl.col('x1').clip(lower_bound=0, upper_bound=2)"},
        }
    elif backend == DataFrameBackends.PYSPARK:
        kwargs = {
            "name": "withColumn",
            "args": ["y1", "F.greatest(F.least('x1', F.lit(2)), F.lit(0))"],
        }

    node = DataFrameMethod(dataframe_api="NATIVE", **kwargs)
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


def test_arg_sql_expr(df0):
    node = DataFrameMethod(
        name="with_columns",
        kwargs={
            "y1": "sql_expr('5 * x1')",
            "y2": "nw.sql_expr('5 * x1')",
        },
    )

    df = node.execute(df0)
    assert_dfs_equal(
        df.select("y1", "y2"), pl.DataFrame({"y1": [5, 10, 15], "y2": [5, 10, 15]})
    )


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

if __name__ == "__main__":
    import polars as pl

    from laktory import models

    df0 = pl.DataFrame({"x": [1.2, 2.1, 2.0, 3.7]})

    node = models.DataFrameMethod(
        name="with_columns",
        kwargs={"xr": "nw.col('x').round().cast(nw.String())"},
    )
    df = node.execute(df0)

    print(df)
