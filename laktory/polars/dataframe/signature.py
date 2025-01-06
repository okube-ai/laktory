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

    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [1, 2, 3], "label": ["a", "b", "c"]})
    print(df.laktory.signature())
    #> DataFrame[x: Int64, label: String]
    ```

    """

    s = ", ".join([f"{k}: {v}" for k, v in df.schema.items()])
    s = f"DataFrame[{s}]"
    return s
