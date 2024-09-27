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
    coalesce: bool = False,
) -> pl.DataFrame:
    """
    Join tables and coalesce join columns. Optionally coalesce columns found
    in both left and other not used in join.

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
    coalesce:
        If `True` columns present in both left and other are coalesced.
        Columns part of the join are always coalesced.

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
    if isinstance(left_on, str):
        left_on = [left_on]
    if isinstance(other_on, str):
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

    suffix = "_right"
    df = left.join(
        other=other,
        on=on,
        left_on=left_on,
        right_on=other_on,
        suffix=suffix,
        how=how,
        coalesce=True,  # to be consistent with Spark smart_join
    )

    if coalesce:
        for c1 in df.columns:
            if c1.endswith(suffix):
                c0 = c1.replace(suffix, "")
                df = df.with_columns(**{c0: pl.coalesce(pl.col(c0), pl.col(c1))})
                df = df.drop(c1)

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
