from typing import Union
import polars as pl

__all__ = [
    "poly1",
    "poly2",
    "scaled_power",
    "roundp",
]


# --------------------------------------------------------------------------- #
# Polynomials                                                                 #
# --------------------------------------------------------------------------- #


def poly1(
    x: pl.Expr,
    a: Union[pl.Expr, float] = 1.0,
    b: Union[pl.Expr, float] = 0.0,
) -> pl.Expr:
    """
    Polynomial function of first degree `a * x + b`

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
    import polars as pl

    df = pl.DataFrame({"x": [9]})
    df = df.with_columns(y=pl.expr.laktory.poly1(pl.col("x"), a=-1, b=2))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ x <i64> 9
    $ y <i64> -7
    '''
    ```
    """
    return a * x + b


def poly2(
    x: pl.Expr,
    a: Union[pl.Expr, float] = 1.0,
    b: Union[pl.Expr, float] = 0.0,
    c: Union[pl.Expr, float] = 0.0,
) -> pl.Expr:
    """
    Polynomial function of second degree `a * x**2 + b * x + c`

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
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [9]})
    df = df.with_columns(y=pl.expr.laktory.poly2(pl.col("x"), a=-1, b=2))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ x <i64> 9
    $ y <f64> -63.0
    '''
    ```
    """
    return a * x**2 + b * x + c


# --------------------------------------------------------------------------- #
# Scaled Power                                                                #
# --------------------------------------------------------------------------- #


def scaled_power(
    x: pl.Expr,
    a: Union[pl.Expr, float] = 1.0,
    n: Union[pl.Expr, float] = 0.0,
) -> pl.Expr:
    """
    Scaled power function `a * x`

    Parameters
    ------
    x:
        Input column
    a:
        Scaling coefficient
    n:
        Exponent

    Returns
    -------
    :
        Output column


    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [9]})
    df = df.with_columns(y=pl.expr.laktory.scaled_power(pl.col("x"), a=-3, n=2))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ x <i64> 9
    $ y <i64> -243
    '''
    ```
    """
    return a * x**n


# --------------------------------------------------------------------------- #
# Rounding                                                                    #
# --------------------------------------------------------------------------- #


def roundp(
    x: pl.Expr,
    p: Union[float, pl.Expr] = 1.0,
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
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame([[0.781], [13.0]], ["x"])
    df = df.with_columns(y=pl.Expr.laktory.roundp(pl.col("x"), p=5))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 2
    $ x <f64> 0.781, 13.0
    $ y <f64> 0.0, 15.0
    '''

    df = df.with_columns(y=pl.Expr.laktory.roundp(pl.col("x"), p=0.25))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 2
    $ x <f64> 0.781, 13.0
    $ y <f64> 0.75, 13.0
    '''
    ```
    """
    return (x / p).round() * p
