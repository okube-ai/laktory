import polars as pl

__all__ = [
    "string_split",
    "uuid",
]


# --------------------------------------------------------------------------- #
# string_split                                                                #
# --------------------------------------------------------------------------- #


def string_split(
    x: pl.Expr,
    pattern: str,
    key: int,
) -> pl.Expr:
    """
    Get substring using separator `pat`.

    Parameters
    ----------
    x:
        Input text series to split
    pattern:
        String or regular expression to split on. If not specified, split on whitespace.
    key:
        Split index to return

    Returns
    -------
    :
        Result

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": ["price_close"]})

    df = df.with_columns(y=pl.Expr.laktory.string_split(pl.col("x"), pattern="_", key=1))
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ x <str> 'price_close'
    $ y <str> 'close'
    '''
    ```
    """
    return x.str.split(by=pattern).list.get(key)


# --------------------------------------------------------------------------- #
# uuid                                                                        #
# --------------------------------------------------------------------------- #


def uuid() -> pl.Expr:
    """
    Create a unique id for each row.

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"id": [0, 1, 2]})
    df = df.with_columns(uuid=pl.Expr.laktory.uuid())
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 3
    Columns: 2
    $ id   <i64> 0, 1, 2
    $ uuid <str> 'd56727ac-0084-4f29-8a7b-6ae72e4c51a0', 'c3f78486-6e18-41ed-9e10-1113d249212c', '22abcee3-3e02-49ff-9359-ee5ff074458f'
    '''
    ```
    """

    def generate_uuid():
        import uuid
        return str(uuid.uuid4())

    return pl.first().apply(lambda _: generate_uuid(), return_dtype=pl.Utf8)
