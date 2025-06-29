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
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df = nw.from_native(pl.DataFrame({"x": [0.781, 13.0]}))
    df = df.with_columns(y=nw.col("x").laktory.roundp(p=5))
    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    | | x     | y    | |
    | |-------|------| |
    | | 0.781 | 0.0  | |
    | | 13.0  | 15.0 | |
    └──────────────────┘
    '''

    df = df.with_columns(y=nw.col("x").laktory.roundp(p=0.25))
    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    | | x     | y    | |
    | |-------|------| |
    | | 0.781 | 0.75 | |
    | | 13.0  | 13.0 | |
    └──────────────────┘
    '''
    ```
    """
    return (self._expr / p).round() * p
