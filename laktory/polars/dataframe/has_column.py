import re
import polars as pl


def has_column(df: pl.DataFrame, col: str) -> bool:
    """
    Check if column `col` exists in `df`

    Parameters
    ----------
    df:
        Input DataFrame
    col
        Column name

    Returns
    -------
    :
        Result

    Examples
    --------

    ```py
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame(
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

    print(df.laktory.has_column("symbol"))
    #> False
    print(df.laktory.has_column("`stock`.`symbol`"))
    #> True
    print(df.laktory.has_column("`prices[2]`.`close`"))
    #> True
    ```
    """
    _col = re.sub(r"\[(\d+)\]", r"[*]", col)
    _col = re.sub(r"`", "", _col)
    return _col in df.laktory.schema_flat()
