import polars as pl

__all__ = [
    "row_number",
]


def row_number() -> pl.Expr:
    """
    Window function: returns a sequential number starting at 1 within a window partition.

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame(
        {
            "x": ["a", "a", "b", "b", "b", "c"],
            "z": ["11", "10", "22", "21", "20", "30"],
        }
    )
    df = df.with_columns(y1=pl.Expr.laktory.row_number())
    df = df.with_columns(y2=pl.Expr.laktory.row_number().over("x"))
    df = df.with_columns(y3=pl.Expr.laktory.row_number().sort_by("z").over("x"))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 6
    Columns: 5
    $ x  <str> 'a', 'a', 'b', 'b', 'b', 'c'
    $ z  <str> '11', '10', '22', '21', '20', '30'
    $ y1 <u32> 1, 2, 3, 4, 5, 6
    $ y2 <u32> 1, 2, 1, 2, 3, 1
    $ y3 <u32> 2, 1, 3, 2, 1, 1
    '''
    ```
    """
    return pl.int_range(1, pl.len() + 1, dtype=pl.UInt32)
