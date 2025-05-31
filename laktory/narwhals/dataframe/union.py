from __future__ import annotations

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
    import polars as pl
    import narwhals as nw

    import laktory  # noqa: F401

    df0 = pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "price": [200.0, 205.0],
            "tstamp": ["2023-09-01", "2023-09-02"],
        }
    )
    df0 = nw.from_native()

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
    if not isinstance(others, (list, tuple, set)):
        others = [others]
    return nw.concat([self._df] + others)
