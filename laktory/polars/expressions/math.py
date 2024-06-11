from typing import Union
import polars as pl

__all__ = [
    "roundp",
]


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
