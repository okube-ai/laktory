import math

import narwhals as nw
import numpy as np
import polars as pl

from laktory.sqlparser import SQLParser

# e = pl
e = nw


def test_binary_operators():
    parser = SQLParser()

    df = pl.DataFrame(
        {
            "x": [1, 2, 6, 4],
            "y": [4, 5, 3, 4],
            "b1": [True, False, True, False],
            "b2": [True, True, False, False],
            "s": [
                {"i": 0, "v": "a"},
                {"i": 1, "v": "b"},
                {"i": 2, "v": "c"},
                {"i": 3, "v": "d"},
            ],
        }
    )
    if e == nw:
        df = nw.from_native(df)

    exprs = [
        ("x + y", e.col("x") + e.col("y")),
        ("x + 4", e.col("x") + e.lit(4)),
        ("x - y", e.col("x") - e.col("y")),
        ("x * y", e.col("x") * e.col("y")),
        ("x / y", e.col("x") / e.col("y")),
        ("x > y", e.col("x") > e.col("y")),
        ("x >= y", e.col("x") >= e.col("y")),
        ("x < y", e.col("x") < e.col("y")),
        ("x <= y", e.col("x") <= e.col("y")),
        ("x == y", e.col("x") == e.col("y")),
        ("x != y", e.col("x") != e.col("y")),
        ("x % y", e.col("x") % e.col("y")),
        ("b1 AND b2", e.col("b1") & e.col("b2")),
        ("b1 OR b2", e.col("b1") | e.col("b2")),
        ("s.v", e.col("s").struct.field("v")),
    ]

    for sql_expr, nw_expr in exprs:
        expr = parser.parse(sql_expr)

        if isinstance(expr, nw.Expr):
            _df = nw.from_native(df)
        else:
            _df = df

        # Evaluate Expression
        _df = df.with_columns(r0=expr, r1=nw_expr)

        # Test
        assert _df["r0"].to_list() == _df["r1"].to_list()

        # print(_df)


def test_struct():
    parser = SQLParser()

    df = pl.DataFrame(
        {
            "x": [
                {"a": 0, "b": 1},
                {"a": 2, "b": 3},
            ]
        }
    )
    if e == nw:
        df = nw.from_native(df)

    exprs = [
        ("x", e.col("x")),
        ("x.b", e.col("x").struct.field("b")),
    ]

    for sql_expr, nw_expr in exprs:
        expr = parser.parse(sql_expr)

        if isinstance(expr, nw.Expr):
            _df = nw.from_native(df)
        else:
            _df = df

        # Evaluate Expression
        _df = df.with_columns(r0=expr, r1=nw_expr)

        # Test
        assert _df["r0"].to_list() == _df["r1"].to_list()


def test_math_functions():
    parser = SQLParser()

    df = pl.DataFrame(
        {
            "x": [1, -2, 6, 4, 2.21235, 2.7],
            "y": [4, 5, 3, 4, 5.1251, 7.2],
        },
        strict=False,
    )
    if e == nw:
        df = nw.from_native(df)

    exprs = [
        ("abs(x)", e.col("x").abs()),
        ("cbrt(x)", e.col("x") ** (1.0 / 3.0)),
        ("ceil(x)", (-1) * ((-1) * e.col("x")) // 1),
        ("exp(x)", math.e ** e.col("x")),
        ("floor(x)", e.col("x") // 1),
        ("ln(x)", e.col("x").map_batches(np.log)),
        ("log(x)", e.col("x").map_batches(np.log10)),
        ("PI()", e.lit(math.pi)),
        ("POW(x, y)", e.col("x") ** e.col("y")),
        ("POW(x, 3)", e.col("x") ** 3),
        ("ROUND(x)", e.col("x").round()),
        # ("ROUND(x, 3)", e.col("x").round(3)),
        # ("sum(x)", e.col("x").sum()),
        # ("avg(x)", e.col("x").mean()),
        # ("count(x)", e.col("x").count()),
    ]

    for sql_expr, nw_expr in exprs:
        print("Parsing ", sql_expr)

        expr = parser.parse(sql_expr)

        # Evaluate Expression
        _df = df.with_columns(r0=expr, r1=nw_expr)
        # print(sql_expr, expr)
        print(_df)

        # Fill NaN
        cols = []
        for c in _df.columns:
            _c = e.col(c)
            cols += [e.when(_c.is_nan()).then(e.lit(-666)).otherwise(_c).alias(c)]
        _df = _df.with_columns(cols)

        # Test
        assert _df["r0"].fill_null(-1).to_list() == _df["r1"].fill_null(-1).to_list()


# def test_string_functions():
#     parser = SQLParser()
#
#     df = pl.DataFrame(
#         {
#             "x": [1, 2, 6, 4, 2.2, 2.7],
#             "y": [4, 5, 3, 4],
#             "s": ["ab", "BA", "cc", "dD"],
#             "b1": [True, False, True, False],
#             "b2": [True, True, False, False],
#         }
#     )
#
#     if e == nw:
#         df = nw.from_native(df)
#
#     exprs = [
#         ("max(x)", e.col("x").max()),
#         ("min(x)", e.col("x").min()),
#         ("sum(x)", e.col("x").sum()),
#         ("avg(x)", e.col("x").mean()),
#         ("count(x)", e.col("x").count()),
#         ("lower(s)", e.col("s").str.to_lowercase()),
#         ("upper(s)", e.col("s").str.to_uppercase()),
#         # ("x || ' ' || y", e.concat_str(e.col("x"), " ", e.col("y"))),
#         # # ("COALESCE(x, 0)", e.coalesce(e.col("x"), e.lit(0))),  # Not currently available from Narwhals
#         # ("CASE WHEN x > 10 THEN 'high' ELSE 'low' END", e.when(e.col("x") > 10).then(e.lit("high")).otherwise(e.lit("low"))),
#         # ("EXTRACT(YEAR FROM x)", e.col("x").dt.year()),
#         # ("CAST(x AS INTEGER)", e.col("x").cast(e.Int32)),
#         # # ("COALESCE(CAST(x AS FLOAT) / y, 0)", (e.coalesce(e.col("x").cast("float") / e.col("y"), e.lit(0))),
#         # # ("UPPER(x || ' ' || COALESCE(y, 'unknown'))", e.concat(e.col("x"), " ", e.col("y").coalesce("unknown")).upper()),
#         # ("CASE WHEN CAST(x AS INTEGER) + y > 100 THEN 'high' ELSE 'low' END", e.when((e.col("x").cast(e.Int32) + e.col("y")) > 100).then(e.lit("high")).otherwise(e.lit("low"))),
#         # ("EXTRACT(YEAR FROM CAST(date_col AS TIMESTAMP)) + z", e.col("date_col").cast(e.Datetime).dt.year() + e.col("z")),
#     ]
#
#     for sql_expr, nw_expr in exprs:
#         expr = parser.parse(sql_expr)
#
#         # Evaluate Expression
#         _df = df.with_columns(r0=expr, r1=nw_expr)
#         # print(sql_expr, expr)
#         print(_df)
#
#         # Test
#
#         assert _df["r0"].to_list() == _df["r1"].to_list()
#
#         # print(_df)
