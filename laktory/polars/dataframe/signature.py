import polars as pl


def signature(df: pl.DataFrame) -> str:
    """
    Returns DataFrame signature.

    Parameters
    ----------
    df:
        Input DataFrame

    Returns
    -------
    :
        Result
    ```
    """

    s = ", ".join([f"{k}: {v}" for k, v in df.schema.items()])
    s = f"DataFrame[{s}]"
    return s
