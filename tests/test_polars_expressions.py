import polars as pl
import numpy as np
import pytest
import laktory

# from uuid import UUID
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# import laktory.spark.functions as LF

df0 = pl.DataFrame(
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

#
# def test_compare(df0=df0):
#     df = df0.withColumn("compare1", LF.compare("x", "a"))
#     df = df.withColumn(
#         "compare2", LF.compare("x", "a", operator=">", where=F.col("a") > 0)
#     )
#     pdf = df.toPandas()
#
#     assert pdf["compare1"].to_list() == [True, False, False]
#     assert pdf["compare2"].to_list() == [False, None, True]
#
#
# def test_poly(df0=df0):
#     df = df0.withColumn("poly1_1", LF.poly1("x", -1, 1.0))
#     df = df.withColumn("poly1_2", LF.poly1("x", F.col("a"), F.col("b")))
#     df = df.withColumn("poly2", LF.poly2("x", 1, c=-1))
#     pdf = df.toPandas()
#
#     assert pdf["poly1_1"].to_list() == [0, -1, -2]
#     assert pdf["poly1_2"].to_list() == [3, -2, 5]
#     assert pdf["poly1_2"].to_list() == [3, -2, 5]
#     assert pdf["poly2"].to_list() == [0, 3, 8]
#
#
# def test_power(df0=df0):
#     df = df0.withColumn("power", LF.scaled_power("x", n=F.col("b")))
#     pdf = df.toPandas()
#
#     assert pdf["power"].to_list() == [1, 1, 9]


def test_roundp(df0=df0):

    df = df0.with_columns(roundp_1=pl.Expr.laktory.roundp(pl.col("pi"), p=0.2))
    df = df.with_columns(roundp_11=pl.col("pi").laktory.roundp(p=0.2))
    df = df.with_columns(roundp_2=pl.Expr.laktory.roundp(pl.col("pi"), p=pl.col("p")))

    assert df["roundp_1"].to_list() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert df["roundp_11"].to_list() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert df["roundp_2"].to_list() == pytest.approx([4, 3.20, 3.15], abs=0.0001)


def test_row_number():

    df = pl.DataFrame({
        "x": ["a", "a", "b", "b", "b", "c"],
        "z": ["11", "10", "22", "21", "20", "30"],
    })
    df = df.with_columns(y1=pl.Expr.laktory.row_number())
    df = df.with_columns(y2=pl.Expr.laktory.row_number().over("x"))
    df = df.with_columns(y3=pl.Expr.laktory.row_number().over("x").sort_by("z"))
    assert df["y1"].to_list() == [1,2,3,4,5,6]
    assert df["y2"].to_list() == [1,2,1,2,3,1]
    assert df["y3"].to_list() == [2,1,3,2,1,1]


def test_sql_expr():

    expr0 = (pl.col("data").struct.field("open") >= 2) & (pl.col("x") > 0) | (
        pl.col("symbol") == "AAPL"
    )
    expr1 = pl.Expr.laktory.sql_expr("data.open >= 2 AND x > 0 OR symbol == 'AAPL'")

    assert str(expr1) == str(expr0)


#
#
# def test_string_split(df0=df0):
#     df = df0.withColumn("split_1", LF.string_split(F.col("word"), "_", 0))
#     df = df.withColumn("split_2", LF.string_split(F.col("word"), "_", 1))
#     pdf = df.toPandas()
#
#     assert pdf["split_1"].to_list() == ["dog", "dog", "dog"]
#     assert pdf["split_2"].to_list() == ["cat", "cat", None]
#
#
# def test_uuid(df0=df0):
#     df = df0.withColumn("uuid", LF.uuid())
#     pdf = df.toPandas()
#
#     for _uuid in pdf["uuid"]:
#         assert str(UUID(_uuid)) == _uuid
#
#     assert pdf["uuid"].nunique() == 3
#
#
# def test_units(df0=df0):
#     df = df0.withColumn("ft", LF.convert_units("x", "m", "ft"))
#     df = df.withColumn("kelvin", LF.convert_units("x", "C", "K"))
#     pdf = df.toPandas()
#
#     assert pdf["ft"].to_list() == [
#         3.280839895013124,
#         6.561679790026248,
#         9.842519685039372,
#     ]
#     assert pdf["kelvin"].to_list() == [274.15, 275.15, 276.15]


if __name__ == "__main__":
    # test_compare()
    # test_poly()
    # test_power()
    # test_roundp()
    test_row_number()
    # test_sql_expr()
    # test_string_split()
    # test_uuid()
    # test_units()
