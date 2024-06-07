from pydantic import BaseModel
from typing import Union
import polars as pl

from laktory._logger import get_logger


logger = get_logger(__name__)


class OrderBy(BaseModel):
    sql_expression: str
    desc: bool = False


def window_filter(
    df,
    partition_by: Union[list[str], None],
    order_by: Union[list[OrderBy], None] = None,
    drop_row_index: bool = True,
    row_index_name: str = "_row_index",
    rows_to_keep: int = 1,
) -> pl.DataFrame:
    """
    Apply spark window-based filtering

    Parameters
    ----------
    df:
        DataFrame
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
    import laktory  # noqa: F401
    import polars as pl

    df0 = pl.DataFrame(
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

    df = df0.laktory.window_filter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=1,
    )

    print(df.glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 4
    $ created_at <datetime[μs]> 2023-01-03 00:00:00, 2023-01-03 00:00:00
    $ symbol              <str> 'APPL', 'GOOL'
    $ price               <f64> 201.5, 201.5
    $ _row_index          <u32> 1, 1
    '''
    ```

    References
    ----------

    * [polars window](https://docs.pola.rs/user-guide/expressions/window/)
    """

    # Row Number
    e = pl.Expr.laktory.row_number()

    # Order by
    if order_by:
        bys = []
        descs = []
        for o in order_by:
            if not isinstance(o, OrderBy):
                o = OrderBy(**o)
            bys += [o.sql_expression]
            descs += [o.desc]
        e = e.sort_by(by=bys, descending=descs)

    # Partition By
    e = e.over(*partition_by)

    # Set rows index
    df = df.with_columns(**{row_index_name: e})

    # Filter
    df = df.filter(pl.col(row_index_name) <= rows_to_keep)
    if drop_row_index:
        df = df.drop(row_index_name)

    return df


if __name__ == "__main__":
    import laktory
    import polars as pl

    df0 = pl.DataFrame(
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

    df = df0.laktory.window_filter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=1,
    )

    print(df.glimpse(return_as_string=True))
