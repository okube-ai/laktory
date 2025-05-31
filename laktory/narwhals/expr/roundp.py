from __future__ import annotations

import narwhals as nw


def roundp(
    self,
    p: float | nw.Expr = 1.0,
) -> nw.Expr:
    """
    Evenly round to the given precision

    Parameters
    ------
    p:
        Precision

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import polars as pl
    import narwhals as nw

    import laktory  # noqa: F401

    df = nw.from_native(pl.DataFrame([[0.781], [13.0]], ["x"]))
    df = df.with_columns(y=nw.Expr.laktory.roundp(pl.col("x"), p=5))
    print(df.to_native().glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 2
    $ x <f64> 0.781, 13.0
    $ y <f64> 0.0, 15.0
    '''

    df = df.with_columns(y=nw.Expr.laktory.roundp(pl.col("x"), p=0.25))
    print(df.to_native().glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 2
    $ x <f64> 0.781, 13.0
    $ y <f64> 0.75, 13.0
    '''
    ```
    """
    return (self._expr / p).round() * p
