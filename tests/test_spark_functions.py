import pandas as pd
import numpy as np
import pytest
from uuid import UUID
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
        "pi": [np.pi] * 3,
        "p": [2, 0.2, 0.05],
        "word": ["dog_cat", "dog_cat_mouse", "dog"],
    },
)
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()

df0 = spark.createDataFrame(pdf)


def test_compare(df0=df0):
    df = df0.withColumn("compare1", LF.compare("x", "a"))
    df = df.withColumn(
        "compare2", LF.compare("x", "a", operator=">", where=F.col("a") > 0)
    )
    pdf = df.toPandas()

    assert pdf["compare1"].tolist() == [True, False, False]
    assert pdf["compare2"].tolist() == [False, None, True]


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


def test_string_split(df0=df0):
    df = df0.withColumn("split_1", LF.string_split(F.col("word"), "_", 0))
    df = df.withColumn("split_2", LF.string_split(F.col("word"), "_", 1))
    pdf = df.toPandas()

    assert pdf["split_1"].tolist() == ["dog", "dog", "dog"]
    assert pdf["split_2"].tolist() == ["cat", "cat", None]


def test_uuid(df0=df0):
    df = df0.withColumn("uuid", LF.uuid())
    pdf = df.toPandas()

    for _uuid in pdf["uuid"]:
        assert str(UUID(_uuid)) == _uuid

    assert pdf["uuid"].nunique() == 3


def test_units(df0=df0):
    df = df0.withColumn("ft", LF.convert_units("x", "m", "ft"))
    df = df.withColumn("kelvin", LF.convert_units("x", "C", "K"))
    pdf = df.toPandas()

    assert pdf["ft"].tolist() == [
        3.280839895013124,
        6.561679790026248,
        9.842519685039372,
    ]
    assert pdf["kelvin"].tolist() == [274.15, 275.15, 276.15]


if __name__ == "__main__":
    test_compare()
    test_poly()
    test_power()
    test_roundp()
    test_string_split()
    test_uuid()
    test_units()
