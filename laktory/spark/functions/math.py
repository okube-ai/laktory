import pyspark.sql.functions as F
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
    x:
        Input column
    a:
        Slope
    b:
        y-intercept

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import laktory.spark.functions as LF

    df = spark.createDataFrame([[9]], ["x"])
    df = df.withColumn("y", LF.poly1("x", a=-1, b=2))
    print(df.show_string())
    '''
    +---+---+
    |  x|  y|
    +---+---+
    |  9| -7|
    +---+---+
    '''
    ```
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
    x:
        Input column
    a:
        x**2 coefficient
    b:
        x**1 coefficient
    c:
        x**0 coefficient

    Returns
    -------
    :
        Output column


    Examples
    --------
    ```py
    import laktory.spark.functions as LF

    df = spark.createDataFrame([[9]], ["x"])
    df = df.withColumn("y", LF.poly2("x", a=-1, b=2))
    print(df.show_string())
    '''
    +---+-----+
    |  x|    y|
    +---+-----+
    |  9|-63.0|
    +---+-----+
    '''
    ```
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
    Power function

    Parameters
    ------
    x:
        Input column
    a:
        Coefficient
    n:
        Exponent

    Returns
    -------
    :
        Output column


    Examples
    --------
    ```py
    import laktory.spark.functions as LF

    df = spark.createDataFrame([[9]], ["x"])
    df = df.withColumn("y", LF.power("x", a=-3, n=2))
    print(df.show_string())
    '''
    +---+------+
    |  x|     y|
    +---+------+
    |  9|-243.0|
    +---+------+
    '''
    ```
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
    return F.round(_col(x) / _lit(p)) * _lit(p)
