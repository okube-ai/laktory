import pandas as pd
import numpy as np
import pytest
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


def test_spark_func_arg(df0=df0):

    df = df0.select(df0.columns)

    new_cols = []
    # Values to parse
    for i, v in enumerate([
        "lit(3)",
        "lit(3.0)",
        "lit('3')",
        "col('x')",
        "col('x') + lit(3)",
        "expr('2*x+a')",
    ]):
        new_col = f"c{i}"
        a = models.SparkFuncArg(value=v)
        df = df.withColumn(new_col, a.eval(spark))
        new_cols += [new_col]

    df.show()

    # Test new column types
    assert df.select(new_cols).schema == T.StructType([
        T.StructField('c0', T.IntegerType(), False),
        T.StructField('c1', T.DoubleType(), False),
        T.StructField('c2', T.StringType(), False),
        T.StructField('c3', T.LongType(), True),
        T.StructField('c4', T.LongType(), True),
        T.StructField('c5', T.LongType(), True),
    ])

    # Values not to parse
    for v0 in [
        "x",
        "3",
    ]:
        v1 = models.SparkFuncArg(value=v0).eval(spark)
        assert v1 == v0


def test_dataframe_df_input(df0=df0):

    df = df0.select(df0.columns)

    # Define Chain
    sc = models.SparkChain(
        nodes=[
            {
                "spark_func_name": "union",
                "spark_func_args": [
                    df1
                ],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    assert df.count() == df0.count() * 2
    assert df.columns == df0.columns


def test_dataframe_table_input(df0=df0):

    df = df0.select(df0.columns)

    sc = models.SparkChain(
        nodes=[
            {
                "spark_func_name": "union",
                "spark_func_args": [
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

    sc = models.SparkChain(
        nodes=[
            {
                "name": "cos_x",
                "type": "double",
                "spark_func_name": "cos",
                "spark_func_args": ["x"],
            },
            {
                "name": "x2",
                "type": "double",
                "sql_expression": "x*2",
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df)

    # Test
    pdf = df.toPandas()
    assert pdf["cos_x"].tolist() == np.cos(pdf["x"]).tolist()
    assert pdf["x2"].tolist() == (pdf["x"]*2).tolist()


def test_udfs(df0=df0):

    df = df0.select(df0.columns)

    def mul3(c):
        return 3*c

    def add_new_col(df, column_name, s=1):
        return df.withColumn(column_name, F.col("x")*s)

    sc = models.SparkChain(
        nodes=[
            {
                "name": "rp",
                "type": "double",
                "spark_func_name": "roundp",
                "spark_func_args": ["p"],
                "spark_func_kwargs": {"p": 0.1}
            },
            {
                "name": "x3",
                "type": "double",
                "spark_func_name": "mul3",
                "spark_func_args": [F.col("x")],
            },
            {
                "spark_func_name": "add_new_col",
                "spark_func_args": ["y"],
                "spark_func_kwargs": {"s": 5},
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df, udfs=[mul3, add_new_col])

    # Test
    pdf = df.toPandas()
    assert pdf["rp"].tolist() == [2.0, 0.2, 0.1]
    assert pdf["x3"].tolist() == (pdf["x"]*3).tolist()
    assert pdf["y"].tolist() == (pdf["x"]*5).tolist()


def test_nested(df0=df0):

    df = df0.select(df0.columns)
    df = df.withColumn("_x2", F.sqrt("x"))

    sc = models.SparkChain(
        nodes=[
            {
                "name": "cos_x",
                "type": "double",
                "spark_func_name": "cos",
                "spark_func_args": ["x"],
            },
            {
                "nodes": [
                    {
                        "spark_func_name": "withColumnRenamed",
                        "spark_func_args": [
                            "x",
                            "x_tmp",
                        ],
                    },
                    {
                        "name": "x2",
                        "type": "double",
                        "spark_func_name": "sqrt",
                        "spark_func_args": ["x_tmp"],
                    },
                ],
            },
            {
                "spark_func_name": "drop",
                "spark_func_args": [
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
        ['x', 'a', 'b', 'c', 'n', 'pi', 'p', 'word', '_x2'],
        ['x', 'a', 'b', 'c', 'n', 'pi', 'p', 'word', '_x2', 'cos_x'],
        ['x_tmp', 'a', 'b', 'c', 'n', 'pi', 'p', 'word', '_x2', 'cos_x', 'x2'],
    ]


def test_exceptions():

    df = df0.select(df0.columns)

    # Input missing - missing not allowed
    sc = models.SparkChain(
        nodes=[
            {
                "name": "cos_x",
                "type": "double",
                "spark_func_name": "cos",
                "spark_func_args": ["col('y')"],
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
                "name": "xy",
                "type": "double",
                "spark_func_name": "coalesce",
                "spark_func_args": ["col('x')", "col('y')"],
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
                "name": "xy",
                "type": "double",
                "spark_func_name": "coalesce",
                "spark_func_args": ["col('z')", "col('y')"],
                "allow_missing_column_args": True,
            },
        ]
    )
    with pytest.raises(MissingColumnsError):
        df = sc.execute(df)


if __name__ == "__main__":
    test_spark_func_arg()
    test_dataframe_df_input()
    test_dataframe_table_input()
    test_column()
    test_udfs()
    test_nested()
    test_exceptions()
