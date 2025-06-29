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
        Names of column(s) to compute row index over.

    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": [0, 0, 0, 1, 1, 1],
                "y": [3, 2, 1, 1, 2, 3],
                "z": [0, 1, 3, 9, 16, 25],
            }
        )
    )

    df = df0.laktory.with_row_index(name="idx", order_by=["y"], partition_by="x")
    print(df)
    '''
    ┌────────────────────┐
    | Narwhals DataFrame |
    |--------------------|
    || x | y | z  | idx ||
    ||---|---|----|-----||
    || 0 | 3 | 0  | 2   ||
    || 0 | 2 | 1  | 1   ||
    || 0 | 1 | 3  | 0   ||
    || 1 | 1 | 9  | 0   ||
    || 1 | 2 | 16 | 1   ||
    || 1 | 3 | 25 | 2   ||
    └────────────────────┘
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
