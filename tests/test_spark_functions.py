import pandas as pd
import numpy as np
import pytest
from uuid import UUID
import laktory
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    df = df0.withColumn("compare1", F.laktory.compare("x", "a"))
    df = df.withColumn(
        "compare2", F.laktory.compare("x", "a", operator=">", where=F.col("a") > 0)
    )
    pdf = df.toPandas()

    assert pdf["compare1"].tolist() == [True, False, False]
    assert pdf["compare2"].tolist() == [False, None, True]


def test_roundp(df0=df0):
    df = df0.withColumn("roundp_1", F.laktory.roundp("pi", p=0.2))
    df = df.withColumn("roundp_2", F.laktory.roundp("pi", p=F.col("p")))
    pdf = df.toPandas()
    assert pdf["roundp_1"].tolist() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert pdf["roundp_2"].tolist() == pytest.approx([4, 3.20, 3.15], abs=0.0001)


def test_string_split(df0=df0):
    df = df0.withColumn("split_1", F.laktory.string_split(F.col("word"), "_", 0))
    df = df.withColumn("split_2", F.laktory.string_split(F.col("word"), "_", 1))
    pdf = df.toPandas()

    assert pdf["split_1"].tolist() == ["dog", "dog", "dog"]
    assert pdf["split_2"].tolist() == ["cat", "cat", None]


def test_uuid(df0=df0):
    df = df0.withColumn("uuid", F.laktory.uuid())
    pdf = df.toPandas()

    for _uuid in pdf["uuid"]:
        assert str(UUID(_uuid)) == _uuid

    assert pdf["uuid"].nunique() == 3


def test_units(df0=df0):
    df = df0.withColumn("ft", F.laktory.convert_units("x", "m", "ft"))
    df = df.withColumn("kelvin", F.laktory.convert_units("x", "C", "K"))
    pdf = df.toPandas()

    assert pdf["ft"].tolist() == [
        3.280839895013124,
        6.561679790026248,
        9.842519685039372,
    ]
    assert pdf["kelvin"].tolist() == [274.15, 275.15, 276.15]


if __name__ == "__main__":
    test_compare()
    test_roundp()
    test_string_split()
    test_uuid()
    test_units()
