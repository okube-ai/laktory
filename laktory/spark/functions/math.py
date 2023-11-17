from typing import Union
import pyspark.sql.functions as F

# from pyspark.sql.functions import pandas_udf
from pyspark.sql.column import Column
from laktory.spark.functions._common import (
    COLUMN_OR_NAME,
    INT_OR_COLUMN,
    FLOAT_OR_COLUMN,
    STRING_OR_COLUMN,
    _col,
    _lit,
)

__all__ = [
    "poly1",
    "poly2",
    "power",
    "roundp",
]


# --------------------------------------------------------------------------- #
# Polynomials                                                                 #
# --------------------------------------------------------------------------- #


def poly1(
    x: COLUMN_OR_NAME,
    a: FLOAT_OR_COLUMN = 1.0,
    b: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of first degree

    Parameters
    ----------
    x: pyspark.sql.functions.column.Column, column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        Slope
    b: float or pyspark.sql.functions.column.Column
        y-intercept

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """

    return _lit(a) * _col(x) + _lit(b)


def poly2(
    x: COLUMN_OR_NAME,
    a: FLOAT_OR_COLUMN = 1.0,
    b: FLOAT_OR_COLUMN = 0.0,
    c: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of second degree

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        x**2 coefficient
    b: float or pyspark.sql.functions.column.Column
        x**1 coefficient
    c: float or pyspark.sql.functions.column.Column
        x**0 coefficient

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column

    Examples
    --------
    >>> 1+1.0
    2.0
    """
    return _lit(a) * _col(x) ** 2 + _lit(b) * _col(x) + _lit(c)


# --------------------------------------------------------------------------- #
# Power                                                                       #
# --------------------------------------------------------------------------- #


def power(
    x: COLUMN_OR_NAME,
    a: FLOAT_OR_COLUMN = 1.0,
    n: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of first degree

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        Coefficient
    n: float or pyspark.sql.functions.column.Column
        Exponent

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """
    return _lit(a) * _col(x) ** _lit(n)


# --------------------------------------------------------------------------- #
# Rounding                                                                    #
# --------------------------------------------------------------------------- #


def roundp(
    x: COLUMN_OR_NAME,
    p: FLOAT_OR_COLUMN = 1.0,
) -> Column:
    """
    Evenly round to the given precision

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    p: float or pyspark.sql.functions.column.Column
        Precision

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """
    # eps0 = 1.0e-16
    # precision = float(precision)
    # if precision < eps0:
    #     raise ValueError("Precision must be greater than 1.0e-16")
    return F.round(_col(x) / _lit(p)) * _lit(p)
