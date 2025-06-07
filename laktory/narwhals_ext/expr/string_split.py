import narwhals as nw


def string_split(
    self,
    pattern: str,
    key: int,
) -> nw.Expr:
    """
    Get substring using separator `pattern`.

    Parameters
    ----------
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
    # ```py
    # import polars as pl
    #
    # import laktory as lk  # noqa: F401
    #
    # df = nw.from_native(pl.DataFrame({"x": ["price_close"]}))
    #
    # df = df.with_columns(y=nw.col("x").laktory.string_split(pattern="_", key=1))
    # print(df.to_native().glimpse(return_as_string=True))
    # '''
    # Rows: 1
    # Columns: 2
    # $ x <str> 'price_close'
    # $ y <str> 'close'
    # '''
    ```
    """
    raise NotImplementedError()
    return self._expr.str.split(by=pattern).list.get(key, null_on_oob=True)
