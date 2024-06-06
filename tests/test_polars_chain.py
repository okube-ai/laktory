# import pandas as pd
import polars as pl
import numpy as np
import pytest
import polars.functions as F
import polars.datatypes as T

from laktory import models
from laktory.exceptions import MissingColumnError
from laktory.exceptions import MissingColumnsError

df = pl.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
        "pi": [np.pi] * 3,
        "p": [2, 0.2, 0.05],
        "word": ["dog_cat", "dog_cat_mouse", "dog"],
    },
)
df0 = df.select(df.columns)
df1 = df.select(df.columns)


def test_polars_func_arg(df0=df0):
    df = df0.select(df0.columns)

    new_cols = []
    # Values to parse
    for i, v in enumerate(
        [
            "lit(3)",
            "lit(3.0)",
            "lit('3')",
            "col('x')",
            "col('x') + lit(3)",
            "sql_expr('2*x+a')",
        ]
    ):
        new_col = f"c{i}"
        a = models.PolarsFuncArg(value=v)
        df = df.with_columns(**{new_col: a.eval()})
        new_cols += [new_col]

    # Test new column types
    data = dict(df.select(new_cols).schema)
    assert data == {
        "c0": T.Int32,
        "c1": T.Float64,
        "c2": T.String,
        "c3": T.Int64,
        "c4": T.Int64,
        "c5": T.Int64,
    }

    # Values not to parse
    for v0 in [
        "x",
        "3",
    ]:
        v1 = models.PolarsFuncArg(value=v0).eval()
        assert v1 == v0


def test_dataframe_df_input(df0=df0):
    df = df0.select(df0.columns)

    # Define Chain
    sc = models.PolarsChain(
        nodes=[
            {
                "polars_func_name": "laktory.union",
                "polars_func_args": [df1],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.height == df0.height * 2
    assert df.columns == df0.columns


def test_dataframe_sql_expression(df0=df0):
    df = df0.select(df0.columns)

    sc = models.PolarsChain(
        nodes=[
            {
                "sql_expression": "SELECT *, x*2 AS x2 FROM self",
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.columns == ["x", "a", "b", "c", "n", "pi", "p", "word", "x2"]
    assert df["x2"].to_list() == (df["x"] * 2).to_list()


def test_dataframe_table_input(df0=df0):
    df = df0.select(df0.columns)

    sc = models.PolarsChain(
        nodes=[
            {
                "polars_func_name": "laktory.union",
                "polars_func_args": [
                    {"mock_df": df},
                ],
            }
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.height == df0.height * 2
    assert df.columns == df0.columns


def test_column(df0=df0):
    df = df0.select(df0.columns)

    sc = models.PolarsChain(
        nodes=[
            {
                "column": {"name": "cos_x", "type": "double"},
                "polars_func_name": "cos",
                "polars_func_args": ["col('x')"],
            },
            {
                "column": {"name": "x2", "type": "double"},
                "sql_expression": "x*2",
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df["cos_x"].to_list() == np.cos(df["x"]).to_list()
    assert df["x2"].to_list() == (df["x"] * 2).to_list()


def test_udfs(df0=df0):
    df = df0.select(df0.columns)

    def mul3(c):
        return 3 * c

    def add_new_col(df, column_name, s=1):
        return df.with_columns(**{column_name: pl.col("x") * s})

    sc = models.PolarsChain(
        nodes=[
            # {
            #     "column": {
            #         "name": "rp",
            #         "type": "double",
            #     },
            #     "polars_func_name": "roundp",
            #     "polars_func_args": ["p"],
            #     "polars_func_kwargs": {"p": 0.1},
            # },
            {
                "column": {"name": "x3", "type": "double"},
                "polars_func_name": "mul3",
                "polars_func_args": [pl.col("x")],
            },
            {
                "polars_func_name": "add_new_col",
                "polars_func_args": ["y"],
                "polars_func_kwargs": {"s": 5},
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df, udfs=[mul3, add_new_col])

    # Test
    assert df["rp"].to_list() == [2.0, 0.2, 0.1]
    assert df["x3"].to_list() == (df["x"] * 3).to_list()
    assert df["y"].to_list() == (df["x"] * 5).to_list()
#
#
# def test_nested(df0=df0):
#     df = df0.select(df0.columns)
#     df = df.withColumn("_x2", F.sqrt("x"))
#
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "column": {"name": "cos_x", "type": "double"},
#                 "spark_func_name": "cos",
#                 "spark_func_args": ["x"],
#             },
#             {
#                 "nodes": [
#                     {
#                         "spark_func_name": "withColumnRenamed",
#                         "spark_func_args": [
#                             "x",
#                             "x_tmp",
#                         ],
#                     },
#                     {
#                         "column": {"name": "x2", "type": "double"},
#                         "spark_func_name": "sqrt",
#                         "spark_func_args": ["x_tmp"],
#                     },
#                 ],
#             },
#             {
#                 "spark_func_name": "drop",
#                 "spark_func_args": [
#                     "x_tmp",
#                 ],
#             },
#         ]
#     )
#
#     # Execute Chain
#     df = sc.execute(df)
#
#     # Test
#     pdf = df.toPandas()
#     assert "x_tmp" not in pdf.columns
#     assert pdf["x2"].to_list() == pdf["_x2"].to_list()
#     assert sc.columns == [
#         ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2"],
#         ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x"],
#         ["x_tmp", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x", "x2"],
#     ]
#
#
# def test_exceptions():
#     df = df0.select(df0.columns)
#
#     # Input missing - missing not allowed
#     sc = models.SparkChain(
#         nodes=[
#             {
#                 "column": {"name": "cos_x", "type": "double"},
#                 "spark_func_name": "cos",
#                 "spark_func_args": ["col('y')"],
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
#                 "spark_func_name": "coalesce",
#                 "spark_func_args": ["col('x')", "col('y')"],
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
#                 "spark_func_name": "coalesce",
#                 "spark_func_args": ["col('z')", "col('y')"],
#                 "allow_missing_column_args": True,
#             },
#         ]
#     )
#     with pytest.raises(MissingColumnsError):
#         df = sc.execute(df)


if __name__ == "__main__":
    test_polars_func_arg()
    test_dataframe_df_input()
    test_dataframe_sql_expression()
    test_dataframe_table_input()
    test_column()
    # test_udfs()
    # test_nested()
    # test_exceptions()
