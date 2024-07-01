from uuid import UUID
import polars as pl
import numpy as np
import pytest
import laktory

df0 = pl.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
        "pi": [np.pi] * 3,
        "p": [2.0, 0.2, 0.05],
        "word": ["dog_cat", "dog_cat_mouse", "dog"],
    },
)


def atest_coalesce(df0=df0):
    return
    # TODO: re-introduce
    df = df0.with_columns(xb0=pl.expr.laktory.coalesce(pl.col("x"), pl.col("b")))
    df = df.with_columns(
        xb1=pl.expr.laktory.coalesce(pl.col("x"), pl.col("b"), available_columns=["b"])
    )
    df = df.with_columns(
        xb2=pl.expr.laktory.coalesce(["x", "b"], available_columns=["b"])
    )
    df = df.with_columns(
        xb3=pl.expr.laktory.coalesce(
            pl.col("x").sqrt(), pl.col("b").sqrt(), available_columns=["b"]
        )
    )
    assert df["xb0"].to_list() == [1, 2, 3]
    assert df["xb1"].to_list() == [2, 0, 2]
    assert df["xb2"].to_list() == [2, 0, 2]
    assert df["xb3"].to_list() == [np.sqrt(2), 0, np.sqrt(2)]


def test_compare(df0=df0):
    df = df0.with_columns(compare1=pl.expr.laktory.compare(pl.col("x"), pl.col("a")))
    df = df.with_columns(
        compare2=pl.expr.laktory.compare(
            pl.col("x"), pl.col("a"), operator=">", where=pl.col("a") > 0
        )
    )
    assert df["compare1"].to_list() == [True, False, False]
    assert df["compare2"].to_list() == [False, None, True]


def test_roundp(df0=df0):

    df = df0.with_columns(roundp_1=pl.Expr.laktory.roundp(pl.col("pi"), p=0.2))
    df = df.with_columns(roundp_11=pl.col("pi").laktory.roundp(p=0.2))
    df = df.with_columns(roundp_2=pl.Expr.laktory.roundp(pl.col("pi"), p=pl.col("p")))

    assert df["roundp_1"].to_list() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert df["roundp_11"].to_list() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert df["roundp_2"].to_list() == pytest.approx([4, 3.20, 3.15], abs=0.0001)


def test_row_number():

    df = pl.DataFrame(
        {
            "x": ["a", "a", "b", "b", "b", "c"],
            "z": ["11", "10", "22", "21", "20", "30"],
        }
    )
    df = df.with_columns(y1=pl.Expr.laktory.row_number())
    df = df.with_columns(y2=pl.Expr.laktory.row_number().over("x"))
    df = df.with_columns(y3=pl.Expr.laktory.row_number().over("x").sort_by("z"))
    assert df["y1"].to_list() == [1, 2, 3, 4, 5, 6]
    assert df["y2"].to_list() == [1, 2, 1, 2, 3, 1]
    assert df["y3"].to_list() == [2, 1, 3, 2, 1, 1]


def test_sql_expr():

    expr0 = (pl.col("data").struct.field("open") >= 2) & (pl.col("x") > 0) | (
        pl.col("symbol") == "AAPL"
    )
    expr1 = pl.Expr.laktory.sql_expr("data.open >= 2 AND x > 0 OR symbol == 'AAPL'")

    assert str(expr1) == str(expr0)


def test_string_split(df0=df0):
    df = df0.with_columns(split_1=pl.Expr.laktory.string_split(pl.col("word"), "_", 0))
    df = df.with_columns(split_2=pl.Expr.laktory.string_split(pl.col("word"), "_", 1))

    assert df["split_1"].to_list() == ["dog", "dog", "dog"]
    assert df["split_2"].to_list() == ["cat", "cat", None]


def test_uuid(df0=df0):
    df = df0.with_columns(uuid=pl.Expr.laktory.uuid())

    for _uuid in df["uuid"]:
        assert str(UUID(_uuid)) == _uuid

    assert df["uuid"].n_unique() == 3


def test_units(df0=df0):
    df = df0.with_columns(ft=pl.Expr.laktory.convert_units(pl.col("x"), "m", "ft"))
    df = df.with_columns(kelvin=pl.Expr.laktory.convert_units(pl.col("x"), "C", "K"))

    assert df["ft"].to_list() == [
        3.280839895013124,
        6.561679790026248,
        9.842519685039372,
    ]
    assert df["kelvin"].to_list() == [274.15, 275.15, 276.15]


if __name__ == "__main__":
    atest_coalesce()
    test_compare()
    test_roundp()
    test_row_number()
    test_sql_expr()
    test_string_split()
    test_uuid()
    test_units()
