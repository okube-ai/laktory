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
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))
    df = df.with_columns(tstamp=nw.laktory.current_timestamp())

    # print(df)
    ```
    """
    return nw.lit(utc_datetime())
