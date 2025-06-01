def signature(self) -> str:
    """
    Returns DataFrame signature.

    Returns
    -------
    :
        Result

    ```py
    import polars as pl

    import laktory  # noqa: F401

    df = pl.DataFrame({"x": [1, 2, 3], "label": ["a", "b", "c"]})
    print(df.laktory.signature())
    # > DataFrame[x: Int64, label: String]
    ```

    """

    s = ", ".join([f"{k}: {v}" for k, v in self._df.schema.items()])
    s = f"DataFrame[{s}]"
    return s
