import polars as pl
from laktory.polars.expressions._common import (
    EXPR_OR_NAME,
    INT_OR_EXPR,
    FLOAT_OR_EXPR,
    STRING_OR_EXPR,
    _to_expr,
)

__all__ = [
    # "add",
    # "sub",
    # "mul",
    # "div",
    # "poly1",
    # "poly2",
    # "scaled_power",
    "roundp",
]


# --------------------------------------------------------------------------- #
# Arithmetics                                                                 #
# --------------------------------------------------------------------------- #

#
# def add(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
# ) -> Column:
#     """
#     Get floating addition `x + a`
#
#     Parameters
#     ----------
#     x:
#         Input column
#     a:
#         Addend
#
#     Returns
#     -------
#     :
#         Output column
#
#     Examples
#     --------
#     ```py
#     import laktory  # noqa: F401
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[8], [6]], ["x"])
#     df = df.withColumn("y", LF.poly1("x", a=2))
#     print(df.show_string())
#     '''
#     +---+----+
#     |  x|   y|
#     +---+----+
#     |  8|16.0|
#     |  6|12.0|
#     +---+----+
#     '''
#     ```
#     """
#     return _col(x) + _lit(a)
#
#
# def sub(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
# ) -> Column:
#     """
#     Get floating subtraction `x - a`
#
#     Parameters
#     ----------
#     x:
#         Input column
#     a:
#         Subtrahend
#
#     Returns
#     -------
#     :
#         Output column
#
#     Examples
#     --------
#     ```py
#     import laktory  # noqa: F401
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[8], [6]], ["x"])
#     df = df.withColumn("y", LF.poly1("x", a=2))
#     print(df.show_string())
#     '''
#     +---+----+
#     |  x|   y|
#     +---+----+
#     |  8|16.0|
#     |  6|12.0|
#     +---+----+
#     '''
#     ```
#     """
#     return _col(x) - _lit(a)
#
#
# def mul(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
# ) -> Column:
#     """
#     Get floating multiplication `x * a`
#
#     Parameters
#     ----------
#     x:
#         Input column
#     a:
#         Multiplier
#
#     Returns
#     -------
#     :
#         Output column
#
#     Examples
#     --------
#     ```py
#     import laktory  # noqa: F401
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[8], [6]], ["x"])
#     df = df.withColumn("y", LF.poly1("x", a=2))
#     print(df.show_string())
#     '''
#     +---+----+
#     |  x|   y|
#     +---+----+
#     |  8|16.0|
#     |  6|12.0|
#     +---+----+
#     '''
#     ```
#     """
#     return _col(x) * _lit(a)
#
#
# def div(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
# ) -> Column:
#     """
#     Get floating division `x / a`
#
#     Parameters
#     ----------
#     x:
#         Input column
#     a:
#         Divider
#
#     Returns
#     -------
#     :
#         Output column
#
#     Examples
#     --------
#     ```py
#     import laktory  # noqa: F401
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[8], [6]], ["x"])
#     df = df.withColumn("y", LF.poly1("x", a=2))
#     print(df.show_string())
#     '''
#     +---+----+
#     |  x|   y|
#     +---+----+
#     |  8|16.0|
#     |  6|12.0|
#     +---+----+
#     '''
#     ```
#     """
#     return _col(x) / _lit(a)
#
#
# # --------------------------------------------------------------------------- #
# # Polynomials                                                                 #
# # --------------------------------------------------------------------------- #
#
#
# def poly1(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
#     b: FLOAT_OR_COLUMN = 0.0,
# ) -> Column:
#     """
#     Polynomial function of first degree `a * x + b`
#
#     Parameters
#     ----------
#     x:
#         Input column
#     a:
#         Slope
#     b:
#         y-intercept
#
#     Returns
#     -------
#     :
#         Output column
#
#     Examples
#     --------
#     ```py
#     import laktory  # noqa: F401
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[9]], ["x"])
#     df = df.withColumn("y", LF.poly1("x", a=-1, b=2))
#     print(df.show_string())
#     '''
#     +---+---+
#     |  x|  y|
#     +---+---+
#     |  9| -7|
#     +---+---+
#     '''
#     ```
#     """
#     return _lit(a) * _col(x) + _lit(b)
#
#
# def poly2(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
#     b: FLOAT_OR_COLUMN = 0.0,
#     c: FLOAT_OR_COLUMN = 0.0,
# ) -> Column:
#     """
#     Polynomial function of second degree `a * x**2 + b * x + c`
#
#     Parameters
#     ------
#     x:
#         Input column
#     a:
#         x**2 coefficient
#     b:
#         x**1 coefficient
#     c:
#         x**0 coefficient
#
#     Returns
#     -------
#     :
#         Output column
#
#
#     Examples
#     --------
#     ```py
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[9]], ["x"])
#     df = df.withColumn("y", LF.poly2("x", a=-1, b=2))
#     print(df.show_string())
#     '''
#     +---+-----+
#     |  x|    y|
#     +---+-----+
#     |  9|-63.0|
#     +---+-----+
#     '''
#     ```
#     """
#     return _lit(a) * _col(x) ** 2 + _lit(b) * _col(x) + _lit(c)
#
#
# # --------------------------------------------------------------------------- #
# # Scaled Power                                                                #
# # --------------------------------------------------------------------------- #
#
#
# def scaled_power(
#     x: COLUMN_OR_NAME,
#     a: FLOAT_OR_COLUMN = 1.0,
#     n: FLOAT_OR_COLUMN = 0.0,
# ) -> Column:
#     """
#     Scaled power function `a * x`
#
#     Parameters
#     ------
#     x:
#         Input column
#     a:
#         Scaling coefficient
#     n:
#         Exponent
#
#     Returns
#     -------
#     :
#         Output column
#
#
#     Examples
#     --------
#     ```py
#     import laktory.spark.functions as LF
#
#     df = spark.createDataFrame([[9]], ["x"])
#     df = df.withColumn("y", LF.scaled_power("x", a=-3, n=2))
#     print(df.show_string())
#     '''
#     +---+------+
#     |  x|     y|
#     +---+------+
#     |  9|-243.0|
#     +---+------+
#     '''
#     ```
#     """
#     return _lit(a) * _col(x) ** _lit(n)
#

# --------------------------------------------------------------------------- #
# Rounding                                                                    #
# --------------------------------------------------------------------------- #


def roundp(
    # self,
    x: EXPR_OR_NAME,
    p: FLOAT_OR_EXPR = 1.0,
) -> pl.Expr:
    """
    Evenly round to the given precision

    Parameters
    ------
    x:
        Input column
    p:
        Precision

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import laktory.spark.functions as LF

    df = spark.createDataFrame([[0.781], [13.0]], ["x"])
    df = df.withColumn("y", LF.roundp("x", p=5))
    print(df.show_string())
    '''
    +-----+----+
    |    x|   y|
    +-----+----+
    |0.781| 0.0|
    | 13.0|15.0|
    +-----+----+
    '''

    df = df.withColumn("y", LF.roundp("x", p=0.25))
    print(df.show_string())
    '''
    +-----+----+
    |    x|   y|
    +-----+----+
    |0.781|0.75|
    | 13.0|13.0|
    +-----+----+
    '''
    ```
    """
    # eps0 = 1.0e-16
    # precision = float(precision)
    # if precision < eps0:
    #     raise ValueError("Precision must be greater than 1.0e-16")

    return (_to_expr(x) / _to_expr(p)).round() * _to_expr(p)
