import pandas as pd
import numpy as np
import pytest

import polars as pl
import laktory.polars as lpl

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
        "data": [
            {"open": 0, "close": 1},
            {"open": 2, "close": 5},
            {"open": 4, "close": 6},
        ],
    },
)


def test_sql_expr(df0=df):

    print(df0)

    expr0 = (pl.col("data").struct.field("open") >= 2) & (pl.col("x") > 0) | (
        pl.col("symbol") == "AAPL"
    )
    expr1 = lpl.sql_expr("data.open >= 2 AND x > 0 OR symbol == 'AAPL'")

    assert str(expr1) == str(expr0)


if __name__ == "__main__":
    test_sql_expr()
