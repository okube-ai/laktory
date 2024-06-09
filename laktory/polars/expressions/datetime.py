import polars as pl

from laktory.datetime import utc_datetime

__all__ = [
    "current_timestamp",
]


# --------------------------------------------------------------------------- #
# compare                                                                     #
# --------------------------------------------------------------------------- #


def current_timestamp() -> pl.Expr:
    """
    Returns the current timestamp at the start of expression evaluation as a
    Datetime column.

    Returns
    -------
    :
        Current timestamp

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [0.45, 0.55]})
    df = df.with_columns(tstamp=pl.expr.laktory.current_timestamp())
    ```
    """

    return pl.lit(utc_datetime())
