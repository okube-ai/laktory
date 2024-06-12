from collections import defaultdict
import polars as pl

from laktory._logger import get_logger


logger = get_logger(__name__)


def smart_join(
    left: pl.DataFrame,
    other: pl.DataFrame,
    how: str = "left",
    on: list[str] = None,
    left_on: list[str] = None,
    other_on: list[str] = None,
) -> pl.DataFrame:
    """
    Join tables and clean up duplicated columns.

    Parameters
    ----------
    left:
        Left side of the join
    other:
        Right side of the join
    how:
        Type of join (left, outer, full, etc.)
    on:
        A list of strings for the columns to join on. The columns must exist
        on both sides.
    left_on:
        Name(s) of the left join column(s).
    other_on:
        Name(s) of the right join column(s).

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df_prices = pl.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL"],
            "price": [200.0, 205.0],
        }
    )

    df_meta = pl.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL"],
            "name": ["Apple", "Google"],
        }
    )

    df = df_prices.laktory.smart_join(
        other=df_meta,
        on=["symbol"],
    )

    print(df.glimpse(return_as_string=True))
    '''
    Rows: 2
    Columns: 3
    $ symbol <str> 'AAPL', 'GOOGL'
    $ price  <f64> 200.0, 205.0
    $ name   <str> 'Apple', 'Google'
    '''
    ```
    """
    logger.info(
        f"Executing {left.laktory.signature()} {how} JOIN {other.laktory.signature()}"
    )

    # Validate inputs
    if left_on or other_on:
        if not other_on:
            raise ValueError("If `left_on` is set, `other_on` should also be set")
        if not left_on:
            raise ValueError("If `other_on` is set, `left_on` should also be set")
    if not (on or left_on or other_on):
        raise ValueError("Either `on` or (`left_on` and `other_on`) should be set")

    # Parse inputs
    if left_on is None:
        left_on = []
    elif isinstance(left_on, str):
        left_on = [left_on]
    if other_on is None:
        other_on = []
    elif isinstance(other_on, str):
        other_on = [other_on]

    # Drop duplicates to prevent adding rows to left
    if on:
        other = other.unique(on, maintain_order=True)

    if on:
        logger.info(f"   ON {on}")
    else:
        logger.info(f"   ON left.{left_on} == other.{other_on}")

    logger.debug(f"Left Schema: {left.laktory.signature()}")
    logger.debug(f"Other Schema: {other.laktory.signature()}")

    df = left.join(
        other=other,
        on=on,
        left_on=left_on,
        right_on=other_on,
        how=how,
        coalesce=None,  # to be consistent with Spark smart_join
    )

    # Copy back coalesced columns to be consistent with Spark smart_join
    for l, o in zip(left_on, other_on):
        if o not in df.columns:
            df = df.with_columns(**{o: pl.col(l)})

    # Drop watermark column
    logger.debug(f"Joined Schema: {df.schema}")

    return df


if __name__ == "__main__":
    from laktory._testing.stockprices import spark
    import pandas as pd

    df_prices = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
            }
        )
    )

    df_meta = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "name": ["Apple", "Google"],
            }
        )
    )

    df = df_prices.smart_join(
        other=df_meta,
        on=["symbol"],
    )
