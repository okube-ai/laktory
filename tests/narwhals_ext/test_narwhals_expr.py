import narwhals as nw
import numpy as np
import polars as pl
import pytest

import laktory as lk  # noqa: F401


def test_roundp():
    df0 = nw.from_native(
        pl.DataFrame(
            {
                "pi": [np.pi] * 3,
                "p": [2.0, 0.2, 0.05],
            }
        )
    )

    df = df0.with_columns(roundp_1=nw.col("pi").laktory.roundp(p=0.2))
    df = df.with_columns(roundp_2=nw.col("pi").laktory.roundp(p=nw.col("p")))

    assert df["roundp_1"].to_list() == pytest.approx([3.2, 3.2, 3.2], abs=0.0001)
    assert df["roundp_2"].to_list() == pytest.approx([4, 3.20, 3.15], abs=0.0001)


def test_units():
    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": [1, 2, 3],
            }
        )
    )

    df = df0.with_columns(ft=nw.col("x").laktory.convert_units("m", "ft"))
    df = df.with_columns(kelvin=nw.col("x").laktory.convert_units("C", "K"))

    assert df["ft"].to_list() == [
        3.280839895013124,
        6.561679790026248,
        9.842519685039372,
    ]
    assert df["kelvin"].to_list() == [274.15, 275.15, 276.15]


#
#
# def atest_coalesce(df0=df0):
#     return
#     # TODO: re-introduce
#     df = df0.with_columns(xb0=pl.expr.laktory.coalesce(pl.col("x"), pl.col("b")))
#     df = df.with_columns(
#         xb1=pl.expr.laktory.coalesce(pl.col("x"), pl.col("b"), available_columns=["b"])
#     )
#     df = df.with_columns(
#         xb2=pl.expr.laktory.coalesce(["x", "b"], available_columns=["b"])
#     )
#     df = df.with_columns(
#         xb3=pl.expr.laktory.coalesce(
#             pl.col("x").sqrt(), pl.col("b").sqrt(), available_columns=["b"]
#         )
#     )
#     assert df["xb0"].to_list() == [1, 2, 3]
#     assert df["xb1"].to_list() == [2, 0, 2]
#     assert df["xb2"].to_list() == [2, 0, 2]
#     assert df["xb3"].to_list() == [np.sqrt(2), 0, np.sqrt(2)]
#
#
#
# def test_string_split(df0=df0):
#     df = df0.with_columns(split_1=pl.Expr.laktory.string_split(pl.col("word"), "_", 0))
#     df = df.with_columns(split_2=pl.Expr.laktory.string_split(pl.col("word"), "_", 1))
#
#     assert df["split_1"].to_list() == ["dog", "dog", "dog"]
#     assert df["split_2"].to_list() == ["cat", "cat", None]
#
