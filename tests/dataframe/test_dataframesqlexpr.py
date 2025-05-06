import polars as pl
import pytest

from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory._testing import get_df1
from laktory.models import DataFrameSQLExpr
from laktory.models import PipelineNodeDataSource

# from laktory import models
# from laktory.exceptions import MissingColumnError
# from laktory.exceptions import MissingColumnsError


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr(backend):
    df0 = get_df0(backend)

    node = DataFrameSQLExpr(sql_expr="SELECT id, 3*x1 AS x3 FROM df")

    df = node.execute(df0)
    assert_dfs_equal(
        df.select("id", "x3"), pl.DataFrame({"id": ["a", "b", "c"], "x3": [3, 6, 9]})
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr_multi(backend):
    df0 = get_df0(backend)
    source = get_df1(backend)

    node = DataFrameSQLExpr(
        sql_expr="SELECT * FROM df LEFT JOIN source on df.id = source.id"
    )
    df = node.execute(df0, source=source)
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))


def test_sql_with_nodes():
    e1 = DataFrameSQLExpr(sql_expr="SELECT * FROM {df}")

    e2 = DataFrameSQLExpr(
        sql_expr="SELECT * FROM {df} UNION SELECT * FROM {nodes.node_01} UNION SELECT * FROM {nodes.node_02}"
    )

    assert e1.data_sources == []
    assert e2.data_sources == [
        PipelineNodeDataSource(node_name="node_01"),
        PipelineNodeDataSource(node_name="node_02"),
    ]


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
