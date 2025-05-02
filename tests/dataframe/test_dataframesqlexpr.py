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
from laktory.models import DataFrameSQLExpr

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


def test_sql_expr(df0, source):
    node = DataFrameSQLExpr(sql_expr="SELECT id, 3*x1 AS x3 FROM df")

    df = node.execute(df0)
    assert_dfs_equal(
        df.select("id", "x3"), pl.DataFrame({"id": ["a", "b", "c"], "x3": [3, 6, 9]})
    )


def test_sql_expr_multi(df0, source):
    source = to_backend(source, df0)

    node = DataFrameSQLExpr(
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
