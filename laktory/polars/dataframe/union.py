import polars as pl


def union(df: pl.DataFrame, other: pl.DataFrame) -> pl.DataFrame:
    """
    Return a new DataFrame containing the union of rows in this and another
    DataFrame.

    Parameters
    ----------
    df:
        Input DataFrame
    other:
        Other DataFrame

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df0 = pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "price": [200.0, 205.0],
            "tstamp": ["2023-09-01", "2023-09-02"],
        }
    )

    df = df0.laktory.union(df0)
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 4
    Columns: 3
    $ symbol <str> 'AAPL', 'AAPL', 'AAPL', 'AAPL'
    $ price  <f64> 200.0, 205.0, 200.0, 205.0
    $ tstamp <str> '2023-09-01', '2023-09-02', '2023-09-01', '2023-09-02'
    '''
    ```
    """
    return pl.concat([df, other])
