import os
import sys
import pandas as pd
import numpy as np
import pytest
import importlib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from laktory import models
from laktory.exceptions import MissingColumnError
from laktory.exceptions import MissingColumnsError

pdf = pd.DataFrame(
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
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()

df0 = spark.createDataFrame(pdf)
df1 = spark.createDataFrame(pdf)


def test_func_arg(df0=df0):
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
            "expr('2*x+a')",
        ]
    ):
        new_col = f"c{i}"
        a = models.SparkChainNodeFuncArg(value=v)
        df = df.withColumn(new_col, a.eval())
        new_cols += [new_col]

    df.show()

    # Test new column types
    assert df.select(new_cols).schema == T.StructType(
        [
            T.StructField("c0", T.IntegerType(), False),
            T.StructField("c1", T.DoubleType(), False),
            T.StructField("c2", T.StringType(), False),
            T.StructField("c3", T.LongType(), True),
            T.StructField("c4", T.LongType(), True),
            T.StructField("c5", T.LongType(), True),
        ]
    )

    # Values not to parse
    for v0 in [
        "x",
        "3",
    ]:
        v1 = models.SparkChainNodeFuncArg(value=v0).eval()
        assert v1 == v0


def test_df_input(df0=df0):
    df = df0.select(df0.columns)

    # Define Chain
    sc = models.SparkChain(
        nodes=[
            {
                "func_name": "union",
                "func_args": [df1],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.count() == df0.count() * 2
    assert df.columns == df0.columns


def test_sql_expression(df0=df0):
    df = df0.select(df0.columns)

    view = """
    CREATE OR REPLACE TEMP VIEW 
        people_view (id, name)
    AS VALUES
        (1, 'john'),
        (2, 'jane')
    ;
    """

    sc = models.SparkChain(
        nodes=[
            {
                "sql_expr": f"{view} SELECT *, x*2 AS x2 FROM {{df}}",
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    print(df)

    # Test
    pdf = df.toPandas()
    assert pdf.columns.tolist() == ["x", "a", "b", "c", "n", "pi", "p", "word", "x2"]
    assert pdf["x2"].tolist() == (pdf["x"] * 2).tolist()

    df.show()


def test_sql_with_nodes():

    sc = models.SparkChain(
        nodes=[
            {
                "sql_expr": "SELECT * FROM {df}",
            },
            {
                "sql_expr": "SELECT * FROM {df} UNION SELECT * FROM {nodes.node_01} UNION SELECT * FROM {nodes.node_02}",
            },
        ]
    )

    assert sc.nodes[0].parsed_sql_expr.node_data_sources == []
    assert sc.nodes[1].parsed_sql_expr.node_data_sources == [
        models.PipelineNodeDataSource(node_name="node_01"),
        models.PipelineNodeDataSource(node_name="node_02"),
    ]


def test_table_input(df0=df0):
    df = df0.select(df0.columns)

    sc = models.SparkChain(
        nodes=[
            {
                "func_name": "union",
                "func_args": [
                    {"mock_df": df},
                ],
            }
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.count() == df0.count() * 2
    assert df.columns == df0.columns


def test_column(df0=df0):
    df = df0.select(df0.columns)
    df = df.withColumn("xs", F.lit([1, 2, 3]))

    sc = models.SparkChain(
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "F.cos('x')",
                },
            },
            {
                "with_column": {
                    "name": "x2",
                    "type": "double",
                    "expr": "F.col('x')*2",
                },
            },
            {
                "with_column": {
                    "name": "xplode",
                    "type": None,
                    "expr": "F.explode('xs')",
                },
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    pdf = df.toPandas()
    assert [(c.name, c.expr.value) for c in sc.nodes[0]._with_columns] == [
        ("cos_x", "F.cos('x')")
    ]
    assert [(c.name, c.expr.value) for c in sc.nodes[1]._with_columns] == [
        ("x2", "F.col('x')*2")
    ]
    assert pdf["cos_x"].tolist() == np.cos(pdf["x"]).tolist()
    assert pdf["x2"].tolist() == (pdf["x"] * 2).tolist()

    # Test explode
    assert pdf["xplode"].tolist() == [1, 2, 3, 1, 2, 3, 1, 2, 3]


def test_udfs(df0=df0):
    df = df0.select(df0.columns)

    def add_new_col(df, column_name, s=1):
        return df.withColumn(column_name, F.col("x") * s)

    sys.path.append(os.path.abspath(os.path.dirname(__file__)))

    module = importlib.import_module("user_defined_functions")
    module = importlib.reload(module)
    udfs = [module.mul3, add_new_col]

    sc = models.SparkChain(
        nodes=[
            {
                "with_column": {
                    "name": "rp",
                    "type": "double",
                    "expr": "F.laktory.roundp('p', p=0.1)",
                },
                # "func_name": "laktory.roundp",
                # "func_args": ["p"],
                # "func_kwargs": {"p": 0.1},
            },
            {
                "with_column": {
                    "name": "x3",
                    "type": "double",
                    "expr": "mul3(col('x'))",
                },
            },
            {
                "func_name": "add_new_col",
                "func_args": ["y"],
                "func_kwargs": {"s": 5},
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df, udfs=udfs)

    # Test
    pdf = df.toPandas()
    assert pdf["rp"].tolist() == [2.0, 0.2, 0.1]
    assert pdf["x3"].tolist() == (pdf["x"] * 3).tolist()
    assert pdf["y"].tolist() == (pdf["x"] * 5).tolist()


def test_nested(df0=df0):
    df = df0.select(df0.columns)
    df = df.withColumn("_x2", F.sqrt("x"))

    sc = models.SparkChain(
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "F.cos('x')",
                },
            },
            {
                "nodes": [
                    {
                        "func_name": "withColumnRenamed",
                        "func_args": [
                            "x",
                            "x_tmp",
                        ],
                    },
                    {
                        "with_column": {
                            "name": "x2",
                            "type": "double",
                            "expr": "F.sqrt('x_tmp')",
                        },
                    },
                ],
            },
            {
                "func_name": "drop",
                "func_args": [
                    "x_tmp",
                ],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    pdf = df.toPandas()
    assert "x_tmp" not in pdf.columns
    assert pdf["x2"].tolist() == pdf["_x2"].tolist()
    assert sc.columns == [
        ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2"],
        ["x", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x"],
        ["x_tmp", "a", "b", "c", "n", "pi", "p", "word", "_x2", "cos_x", "x2"],
    ]


def atest_exceptions():

    # TODO: Re-enable when coalesce is ready
    return

    df = df0.select(df0.columns)

    # Input missing - missing not allowed
    sc = models.SparkChain(
        nodes=[
            {
                "column": {"name": "cos_x", "type": "double"},
                "func_name": "cos",
                "func_args": ["col('y')"],
                "allow_missing_column_args": False,
            },
        ]
    )
    with pytest.raises(MissingColumnError):
        df = sc.execute(df)

    # Input missing - missing allowed
    sc = models.SparkChain(
        nodes=[
            {
                "column": {"name": "xy", "type": "double"},
                "func_name": "coalesce",
                "func_args": ["col('x')", "col('y')"],
                "allow_missing_column_args": True,
            },
        ]
    )
    df = sc.execute(df)
    assert "xy" in df.columns

    # All inputs missing
    sc = models.SparkChain(
        nodes=[
            {
                "column": {"name": "xy", "type": "double"},
                "func_name": "coalesce",
                "func_args": ["col('z')", "col('y')"],
                "allow_missing_column_args": True,
            },
        ]
    )
    with pytest.raises(MissingColumnsError):
        df = sc.execute(df)


if __name__ == "__main__":
    test_func_arg()
    test_df_input()
    test_sql_expression()
    test_sql_with_nodes()
    test_table_input()
    test_column()
    test_udfs()
    test_nested()
    atest_exceptions()
