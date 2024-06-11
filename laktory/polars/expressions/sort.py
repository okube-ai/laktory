from typing import Union
import polars as pl

__all__ = [
    # "coalesce",
    "row_number",
]


# def coalesce(exprs: Union[str, pl.Expr, list[str], list[pl.Expr]], *more_exprs, available_columns=None):
#
#     if not available_columns:
#         return pl.coalesce(exprs, *more_exprs)
#
#     # Parse inputs
#     if not isinstance(exprs, list):
#         exprs = [exprs]
#     exprs = [e for e in exprs] + [e for e in more_exprs]
#
#     _exprs = []
#     for e in exprs:
#         if isinstance(e, str) and e in available_columns:
#             _exprs += [e]
#
#         elif isinstance(e, pl.Expr):
#             found = all([c in available_columns for c in e.meta.root_names()])
#             if found:
#                 _exprs += [e]
#
#     return pl.coalesce(_exprs)


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
