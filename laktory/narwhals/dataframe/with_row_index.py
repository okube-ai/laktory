import narwhals as nw

from laktory.typing import AnyFrame


def with_row_index(
    self,
    name: str = "index",
    order_by: str | list[str] | None = None,
    partition_by: str | list[str] | None = None,
) -> AnyFrame:
    """
    Insert column which enumerates rows by partition and according to `order_by`.

    Parameters
    ----------
    name:
        The name of the column as a string.
    order_by:
        Column(s) to order window functions by. For lazy dataframes, this argument is required.
    partition_by:
        Names of columns to compute row index over.

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

    df = self._df
    is_lazy = isinstance(df, nw.LazyFrame)

    if is_lazy and order_by is None:
        raise ValueError("Argument `order_by` required to LazyFrame")

    # Narwhals base with_row_index method.
    if not (order_by or partition_by):
        return df.with_row_index(name=name)

    tmp = "__lk_row_idx"
    df = df.with_columns(nw.lit(1).alias(tmp))
    if partition_by is None:
        partition_by = tmp
    if isinstance(partition_by, str):
        partition_by = [partition_by]

    e = nw.col(tmp).cum_count().over(*partition_by, order_by=order_by)

    return df.with_columns((e - 1).alias(name)).drop(tmp)
