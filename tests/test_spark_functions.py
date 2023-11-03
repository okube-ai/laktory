import pandas as pd
import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import laktory.spark.functions as LF

pdf = pd.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
        "pi": [np.pi]*3,
        "p": [2, 0.2, 0.05],
     },
)
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()

df0 = spark.createDataFrame(pdf)


def test_poly(df0=df0):
    df = df0.withColumn("poly1_1", LF.poly1("x", -1, 1.0))
    df = df.withColumn("poly1_2", LF.poly1("x", F.col("a"), F.col("b")))
    df = df.withColumn("poly2", LF.poly2("x", 1, c=-1))
    pdf = df.toPandas()

    assert pdf["poly1_1"].tolist() == [0, -1, -2]
    assert pdf["poly1_2"].tolist() == [3, -2, 5]
    assert pdf["poly1_2"].tolist() == [3, -2, 5]
    assert pdf["poly2"].tolist() == [0, 3, 8]


def test_power(df0=df0):
    df = df0.withColumn("power", LF.power("x", n=F.col("b")))
    pdf = df.toPandas()

    assert pdf["power"].tolist() == [1, 1, 9]


def test_roundp(df0=df0):
    df = df0.withColumn("roundp_1", LF.roundp("pi", p=0.2))
    df = df.withColumn("roundp_2", LF.roundp("pi", p=F.col("p")))
    pdf = df.toPandas()
    assert pdf["roundp_1"].tolist() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert pdf["roundp_2"].tolist() == pytest.approx([4, 3.20, 3.15], abs=0.0001)


if __name__ == "__main__":
    test_poly()
    test_power()
    test_roundp()

