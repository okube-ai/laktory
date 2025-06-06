import narwhals as nw

from laktory.typing import AnyFrame


def union(self, others: AnyFrame | list[AnyFrame]) -> AnyFrame:
    """
    Return a new DataFrame containing the union of rows in this and another
    DataFrame.

    Parameters
    ----------
    others:
        Other DataFrame(s)

    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": [0, 1],
            }
        )
    )

    df = df0.laktory.union(df0)
    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |      | x |       |
    |      |---|       |
    |      | 0 |       |
    |      | 1 |       |
    |      | 0 |       |
    |      | 1 |       |
    └──────────────────┘
    '''
    ```
    """
    if not isinstance(others, (list, tuple, set)):
        others = [others]
    return nw.concat([self._df] + others)
