from collections import defaultdict
from pyspark.sql.dataframe import DataFrame

from laktory._logger import get_logger
from laktory.spark.dataframe.watermark import watermark


logger = get_logger(__name__)


def laktory_join(
    left: DataFrame,
    other: DataFrame,
    how: str = "left",
    on: list[str] = None,
    on_expression: str = None,
    time_constraint_interval_lower: str = "60 seconds",
    time_constraint_interval_upper: str = None,
) -> DataFrame:
    """
    Laktory table join

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
    time_constraint_interval_lower:
        Lower bound for a spark streaming event-time constraint
    time_constraint_interval_upper:
        Upper bound for a spark streaming event-time constraint

    Examples
    --------
    ```py
    from laktory import models

    table = models.Table(
        name="slv_star_stock_prices",
        builder={
            "layer": "SILVER",
            "table_source": {
                "name": "slv_stock_prices",
            },
            "joins": [
                {
                    "other": {
                        "name": "slv_stock_metadata",
                        "read_as_stream": False,
                        "selects": ["symbol", "currency", "first_trader"],
                    },
                    "on": ["symbol"],
                }
            ],
        },
    )

    table = models.Table(
        name="slv_star_stock_prices",
        builder={
            "layer": "SILVER",
            "table_source": {
                "name": "slv_stock_prices",
            },
            "joins": [
                {
                    "other": {
                        "name": "slv_stock_metadata",
                        "read_as_stream": False,
                        "selects": ["symbol", "currency", "first_trader"],
                    },
                    "on_expression": "left.symbol == other.symbol",
                }
            ],
        },
    )
    ```

    References
    ----------

    * [pyspark join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)
    * [spark streaming join](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#inner-joins-with-optional-watermarking)
    """
    import pyspark.sql.functions as F

    # Parse inputs
    if on is None:
        on = []

    logger.info(f"Executing {left} {how} JOIN {other}")

    wml = watermark(left)
    wmo = watermark(other)

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

    logger.info(f"Left Schema:")
    left.printSchema()

    logger.info(f"Other Schema:")
    other.printSchema()

    df = (
        left.alias("left")
        .join(
            other=other.alias("other"),
            on=F.expr(_join),
            how=how,
        )
        .drop()
    )

    # Find duplicated columns (because of join)
    d = defaultdict(lambda: 0)
    for c in df.columns:
        d[c] += 1

    # Drop duplicated columns
    for c, v in d.items():
        if v < 2 or c not in _join:
            continue
        df = df.withColumn("__tmp", F.coalesce(f"left.{c}", f"other.{c}"))
        df = df.drop(c)
        df = df.withColumn(c, F.col("__tmp"))
        df = df.drop("__tmp")

    # Drop watermark column
    if wmo is not None:
        df = df.drop(F.col(f"other._other_wc"))
    logger.info(f"Joined Schema:")
    df.printSchema()

    return df
