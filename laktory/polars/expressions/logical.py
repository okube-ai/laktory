from typing import Union
import polars as pl

__all__ = [
    "compare",
]


# --------------------------------------------------------------------------- #
# compare                                                                     #
# --------------------------------------------------------------------------- #


def compare(
    x: pl.Expr,
    y: Union[pl.Expr, float, str, bool] = 0,
    where: pl.Expr = None,
    operator: str = "==",
    default: Union[pl.Expr, float, str, bool] = None,
) -> pl.Expr:
    """
    Compare a column `x` and a value or another column `y` using
    `operator`. Comparison can be limited to `where` and assigned
    `default` elsewhere.

    output = `x` `operator` `y`

    Parameters
    ---------
    x :
        Base column to compare
    y :
        Column to compare to
    where:
        Where to apply the comparison
    operator: str
        Operator for comparison
    default:
        Default value to be applied when `where` is `False`

    Returns
    -------
    :
        Comparison result

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [0.45, 0.55]})
    df = df.with_columns(
        y=pl.expr.laktory.compare(
            pl.col("x"),
            pl.lit(0.5),
            operator=">",
        )
    )
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 2
    $ x  <f64> 0.45, 0.55
    $ y <bool> False, True
    '''
    ```
    """
    if operator == "==":
        c = x == y
    elif operator == "!=":
        c = x != y
    elif operator == "<":
        c = x < y
    elif operator == "<=":
        c = x <= y
    elif operator == ">":
        c = x > y
    elif operator == ">=":
        c = x >= y
    else:
        raise ValueError(f"Operator '{operator}' is not supported.")

    if where is not None:
        if default is None:
            default = pl.lit(None)

        c = pl.when(where).then(c).otherwise(default)

    return c
