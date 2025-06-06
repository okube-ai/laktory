import re


def has_column(self, col: str) -> bool:
    """
    Check if column `col` exists in `df`

    Parameters
    ----------
    col
        Column name

    Returns
    -------
    :
        Result

    Examples
    --------

    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df = nw.from_native(
        pl.DataFrame(
            {
                "indexx": [1, 2, 3],
                "stock": [
                    {"symbol": "AAPL", "name": "Apple"},
                    {"symbol": "MSFT", "name": "Microsoft"},
                    {"symbol": "GOOGL", "name": "Google"},
                ],
                "prices": [
                    [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
                    [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
                    [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
                ],
            }
        )
    )

    print(df.laktory.has_column("symbol"))
    # > False
    print(df.laktory.has_column("`stock`.`symbol`"))
    # > True
    print(df.laktory.has_column("`prices[2]`.`close`"))
    # > True
    ```
    """
    _col = re.sub(r"\[(\d+)\]", r"[*]", col)
    _col = re.sub(r"`", "", _col)
    return _col in self._df.laktory.schema_flat()
