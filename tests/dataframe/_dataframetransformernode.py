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
from laktory.models import DataFrameTransformerNode

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
    node = DataFrameTransformerNode(
        func_name="select",
        func_args=["id"],
    )
    df = node.execute(df0)
    assert df.columns == ["id"]


def test_kwarg_string(df0):
    node = DataFrameTransformerNode(
        func_name="with_columns",
        func_kwargs={
            "y1": "x1",
        },
    )
    df = node.execute(df0)
    assert df.columns == ["id", "x1", "y1"]


def test_arg_source(df0, source):
    source = to_backend(source, df0)

    node = DataFrameTransformerNode(
        func_name="join",
        func_args=[source],
        func_kwargs={
            "on": "id",
            "how": "left",
        },
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))


def test_arg_nw_expr(df0):
    node = DataFrameTransformerNode(
        func_name="with_columns",
        func_kwargs={"y1": "nw.col('x1').clip(lower_bound=0, upper_bound=2)"},
    )
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


def test_arg_native_expr(df0):
    backend = get_backend(df0)

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

    node = DataFrameTransformerNode(dataframe_api="NATIVE", **kwargs)
    df = node.execute(df0)
    assert_dfs_equal(df.select("y1"), pl.DataFrame({"y1": [1, 2, 2]}))


def test_arg_sql_expr(df0):
    node = DataFrameTransformerNode(
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


def test_sql_expr(df0, source):
    node = DataFrameTransformerNode(sql_expr="SELECT id, 3*x1 AS x3 FROM df")

    df = node.execute(df0)
    assert_dfs_equal(
        df.select("id", "x3"), pl.DataFrame({"id": ["a", "b", "c"], "x3": [3, 6, 9]})
    )


def test_sql_expr_multi(df0, source):
    source = to_backend(source, df0)

    node = DataFrameTransformerNode(
        sql_expr="SELECT * FROM df LEFT JOIN source on df.id = source.id"
    )
    df = node.execute(df0, source=source)
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))


# def test_sql_with_nodes():
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "sql_expr": "SELECT * FROM {df}",
#             },
#             {
#                 "sql_expr": "SELECT * FROM {df} UNION SELECT * FROM {nodes.node_01} UNION SELECT * FROM {nodes.node_02}",
#             },
#         ]
#     )
#
#     assert sc.nodes[0].parsed_sql_expr.data_sources == []
#     assert sc.nodes[1].parsed_sql_expr.data_sources == [
#         models.PipelineNodeDataSource(node_name="node_01"),
#         models.PipelineNodeDataSource(node_name="node_02"),
#     ]
#
#
# def test_udfs(df0=df0):
#     df = df0.select(df0.columns)
#
#     def add_new_col(df, column_name, s=1):
#         return df.withColumn(column_name, F.col("x") * s)
#
#     sys.path.append(os.path.abspath(os.path.dirname(__file__)))
#
#     module = importlib.import_module("user_defined_functions")
#     module = importlib.reload(module)
#     udfs = [module.mul3, add_new_col]
#
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "with_column": {
#                     "name": "rp",
#                     "type": "double",
#                     "expr": "F.laktory.roundp('p', p=0.1)",
#                 },
#                 # "func_name": "laktory.roundp",
#                 # "func_args": ["p"],
#                 # "func_kwargs": {"p": 0.1},
#             },
#             {
#                 "with_column": {
#                     "name": "x3",
#                     "type": "double",
#                     "expr": "mul3(col('x'))",
#                 },
#             },
#             {
#                 "func_name": "add_new_col",
#                 "func_args": ["y"],
#                 "func_kwargs": {"s": 5},
#             },
#         ]
#     )
#
#     # Execute Chain
#     df = sc.execute(df, udfs=udfs)
#
#     # Test
#     pdf = df.toPandas()
#     assert pdf["rp"].tolist() == [2.0, 0.2, 0.1]
#     assert pdf["x3"].tolist() == (pdf["x"] * 3).tolist()
#     assert pdf["y"].tolist() == (pdf["x"] * 5).tolist()
#
#
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

    node = models.DataFrameTransformerNode(
        func_name="with_columns",
        func_kwargs={"xr": "nw.col('x').round().cast(nw.String())"},
    )
    df = node.execute(df0)

    print(df)
