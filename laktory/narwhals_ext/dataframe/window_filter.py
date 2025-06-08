import narwhals as nw

from laktory._logger import get_logger
from laktory.typing import AnyFrame

logger = get_logger(__name__)


def window_filter(
    self,
    partition_by: str | list[str] | None,
    order_by: str | list[str] | None = None,
    drop_row_index: bool = True,
    row_index_name: str = "_row_index",
    rows_to_keep: int = 1,
) -> AnyFrame:
    """
    Apply spark window-based filtering

    Parameters
    ----------
    partition_by
        Defines the columns used for grouping into Windows
    order_by:
        Defines the column used for sorting before dropping rows
    drop_row_index:
        If `True`, the row index column is dropped
    row_index_name:
        Group-specific and sorted rows index name
    rows_to_keep:
        How many rows to keep per window


    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df0 = nw.from_native(
        pl.DataFrame(
            [
                ["2023-01-01T00:00:00Z", "APPL", 200.0],
                ["2023-01-02T00:00:00Z", "APPL", 202.0],
                ["2023-01-03T00:00:00Z", "APPL", 201.5],
                ["2023-01-01T00:00:00Z", "GOOL", 200.0],
                ["2023-01-02T00:00:00Z", "GOOL", 202.0],
                ["2023-01-03T00:00:00Z", "GOOL", 201.5],
            ],
            ["created_at", "symbol", "price"],
        ).with_columns(pl.col("created_at").cast(pl.Datetime))
    )

    df = df0.laktory.window_filter(
        partition_by="symbol",
        order_by=["created_at"],
        drop_row_index=False,
        rows_to_keep=1,
    )

    print(df)
    '''
    ┌─────────────────────────────────────────────────────┐
    |                 Narwhals DataFrame                  |
    |-----------------------------------------------------|
    || created_at          | symbol | price | _row_index ||
    ||---------------------|--------|-------|------------||
    || 2023-01-01 00:00:00 | APPL   | 200.0 | 0          ||
    || 2023-01-01 00:00:00 | GOOL   | 200.0 | 0          ||
    └─────────────────────────────────────────────────────┘
    '''
    ```
    """
    if rows_to_keep < 1:
        raise ValueError("`rows_to_keep` must be >= 1")

    # Row Index
    df = self._df.laktory.with_row_index(
        name=row_index_name, partition_by=partition_by, order_by=order_by
    )

    # Filter
    df = df.filter(nw.col(row_index_name) <= rows_to_keep - 1)
    if drop_row_index:
        df = df.drop(row_index_name)

    return df
