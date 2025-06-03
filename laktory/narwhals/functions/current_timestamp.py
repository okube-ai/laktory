import narwhals as nw

from laktory.datetime import utc_datetime


def current_timestamp() -> nw.Expr:
    """
    Returns the current timestamp at the start of expression evaluation as a timestamp.

    Returns
    -------
    :
        Current timestamp

    Examples
    --------
    ```py
    import polars as pl
    import narwhals as nw

    import laktory  # noqa: F401

    df = nw.from_native(pl.DataFrame({"x": [0.45, 0.55]}))
    df = df.with_columns(tstamp=nw.expr.laktory.current_timestamp())
    ```
    """
    return nw.lit(utc_datetime())
