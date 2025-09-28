import narwhals as nw
import pytest

from laktory._testing import get_df0
from laktory.models import DataFrameColumnExpr

from ..conftest import assert_dfs_equal


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_df_expr(backend):
    df0 = get_df0(backend)

    e = DataFrameColumnExpr(
        expr="col('x1')+lit(1)",
        dataframe_backend=backend,
        dataframe_api="NATIVE",
    )
    assert e.type == "DF"

    dft = df0.with_columns(y=nw.col("x1") + nw.lit(1))

    if backend == "POLARS":
        df = nw.from_native(df0.to_native().with_columns(y=e.to_expr()))
    elif backend == "PYSPARK":
        df = nw.from_native(df0.to_native().withColumn("y", e.to_expr()))
    else:
        raise ValueError()
    assert_dfs_equal(df, dft)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_df_expr_narwhals(backend):
    df0 = get_df0(backend)

    e = DataFrameColumnExpr(
        expr="col('x1')+lit(1)",
        dataframe_api="NARWHALS",
    )
    assert e.type == "DF"

    dft = df0.with_columns(y=nw.col("x1") + nw.lit(1))
    df = df0.with_columns(y=e.to_expr())
    assert_dfs_equal(df, dft)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr(backend):
    df0 = get_df0(backend)

    e = DataFrameColumnExpr(
        expr="x1 + 1",
        dataframe_backend=backend,
        dataframe_api="NATIVE",
    )
    assert e.type == "SQL"

    dft = df0.with_columns(y=nw.col("x1") + nw.lit(1))

    if backend == "POLARS":
        df = nw.from_native(df0.to_native().with_columns(y=e.to_expr()))
    elif backend == "PYSPARK":
        df = nw.from_native(df0.to_native().withColumn("y", e.to_expr()))
    else:
        raise ValueError()
    assert_dfs_equal(df, dft)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr_narwhals(backend):
    df0 = get_df0(backend)

    e = DataFrameColumnExpr(
        expr="x1 + 1",
        dataframe_api="NARWHALS",
    )
    assert e.type == "SQL"

    dft = df0.with_columns(y=nw.col("x1") + nw.lit(1))
    df = df0.with_columns(y=e.to_expr())
    assert_dfs_equal(df, dft)


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
