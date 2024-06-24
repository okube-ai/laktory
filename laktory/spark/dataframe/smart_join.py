from collections import defaultdict
from pyspark.sql.dataframe import DataFrame

from laktory._logger import get_logger
from laktory.spark.dataframe.watermark import watermark
from laktory.spark.dataframe.watermark import Watermark


logger = get_logger(__name__)


def smart_join(
    left: DataFrame,
    other: DataFrame,
    how: str = "left",
    on: list[str] = None,
    on_expression: str = None,
    left_on: list[str] = None,
    left_watermark: Watermark = None,
    other_on: list[str] = None,
    other_watermark: Watermark = None,
    time_constraint_interval_lower: str = "60 seconds",
    time_constraint_interval_upper: str = None,
    coalesce: bool = False,
) -> DataFrame:
    """
     Join tables and coalesce join columns. Optionally coalesce columns found
    in both left and other not used in join. Support for watermarking for
    streaming joins.

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
    on_expression:
        String expression the join on condition. The expression can include
        `left` and `other` dataframe references.
    left_on:
        Name(s) of the left join column(s).
    left_watermark
        Watermark for left dataframe
    other_on:
        Name(s) of the right join column(s).
    other_watermark
        Watermark for other dataframe
    time_constraint_interval_lower:
        Lower bound for a spark streaming event-time constraint
    time_constraint_interval_upper:
        Upper bound for a spark streaming event-time constraint
    coalesce:
        If `True` columns present in both left and other are coalesced.

    Examples
    --------
    ```py
    import pandas as pd
    import laktory  # noqa: F401

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

    df = df_prices.laktory.smart_join(
        other=df_meta,
        on=["symbol"],
    )

    print(df.toPandas().to_string())
    '''
       price    name symbol
    0  200.0   Apple   AAPL
    1  205.0  Google  GOOGL
    '''
    ```

    References
    ----------

    * [pyspark join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)
    * [spark streaming join](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#inner-joins-with-optional-watermarking)
    """
    import pyspark.sql.functions as F

    logger.info(f"Executing {left} {how} JOIN {other}")

    # Validate inputs
    if left_on or other_on:
        if not other_on:
            raise ValueError("If `left_on` is set, `other_on` should also be set")
        if not left_on:
            raise ValueError("If `other_on` is set, `left_on` should also be set")
    if not (on or left_on or on_expression):
        raise ValueError(
            "Either `on` or (`left_on` and `other_on`) or `on_expression` should be set"
        )

    # Parse inputs
    if on is None:
        on = []
    elif isinstance(on, str):
        on = [on]
    if left_on is None:
        left_on = []
    elif isinstance(left_on, str):
        left_on = [left_on]
    if other_on is None:
        other_on = []
    elif isinstance(other_on, str):
        other_on = [other_on]

    wml = left_watermark
    wmo = other_watermark
    if wml is not None and not isinstance(wml, Watermark):
        wml = Watermark(**wml)
    if wmo is not None and not isinstance(wmo, Watermark):
        wmo = Watermark(**wmo)

    # Set watermarks
    if wml is None:
        try:
            wml = watermark(left)
        except Exception as e:
            logger.warn(f"Could not fetch wartermark from left dataframe: {e}")
    else:
        left = left.withWatermark(wml.column, wml.threshold)
    if wmo is None:
        try:
            wmo = watermark(other)
        except Exception as e:
            logger.warn(f"Could not fetch wartermark from other dataframe: {e}")
    else:
        other = other.withWatermark(wmo.column, wmo.threshold)

    # Add watermark
    other_cols = []
    if wmo is not None:
        other_cols += [F.col(wmo.column).alias("_other_wc")]
    other_cols += [F.col(c) for c in other.columns]
    other = other.select(other_cols)

    # Drop duplicates to prevent adding rows to left
    if on:
        other = other.dropDuplicates(on)

    _join = []
    for c in on:
        _join += [f"left.{c} == other.{c}"]
    for l, o in zip(left_on, other_on):
        _join += [f"left.{l} == other.{o}"]
    if on_expression:
        _join += [on_expression]

    if wmo is not None:
        if time_constraint_interval_lower:
            _join += [
                f"left.{wml.column} >= other._other_wc - interval {time_constraint_interval_lower}"
            ]
        if time_constraint_interval_upper:
            _join += [
                f"left.{wml.column} <= other._other_wc + interval {time_constraint_interval_upper}"
            ]
    _join = " AND ".join(_join)

    logger.info(f"   ON {_join}")
    logger.debug(f"Left Schema: {left.schema}")
    logger.debug(f"Other Schema: {other.schema}")

    df = left.alias("left").join(
        other=other.alias("other"),
        on=F.expr(_join),
        how=how,
    )

    # Find duplicated columns (because of join)
    d = defaultdict(lambda: 0)
    for c in df.columns:
        d[c] += 1

    # Drop duplicated columns
    for c, v in d.items():
        if v < 2:
            continue

        if not (coalesce or c in _join):
            continue
        df = df.withColumn("__tmp", F.coalesce(f"left.{c}", f"other.{c}"))
        df = df.drop(c)
        df = df.withColumn(c, F.col("__tmp"))
        df = df.drop("__tmp")

    # Drop watermark column
    if wmo is not None:
        df = df.drop(F.col(f"other._other_wc"))
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
