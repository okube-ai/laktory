import pyspark.sql.functions as F
from pyspark.sql.column import Column

from laktory.spark.functions._common import COLUMN_OR_NAME
from laktory.spark.functions._common import _col

__all__ = [
    "string_split",
    "uuid",
]


# --------------------------------------------------------------------------- #
# string_split                                                                #
# --------------------------------------------------------------------------- #


def string_split(
    x: COLUMN_OR_NAME,
    pattern: str,
    key: int,
) -> Column:
    """
    Get substring using separator `pat`.

    Parameters
    ----------
    x:
        Input text series to split
    pattern:
        String or regular expression to split on. If not specified, split on whitespace.
    key:
        Split index to return

    Returns
    -------
    :
        Result

    Examples
    --------
    ```py
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    df = spark.range(1).withColumn("x", F.lit("price_close"))
    df = df.withColumn("y", F.laktory.string_split("x", pattern="_", key=1))
    print(df.laktory.show_string())
    '''
    +---+-----------+-----+
    | id|          x|    y|
    +---+-----------+-----+
    |  0|price_close|close|
    +---+-----------+-----+
    '''
    ```
    """
    return F.split(_col(x), pattern=pattern).getItem(key)


# --------------------------------------------------------------------------- #
# uuid                                                                        #
# --------------------------------------------------------------------------- #


def uuid() -> Column:
    """
    Create a unique id for each row.

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import pyspark.sql.functions as F

    import laktory  # noqa: F401

    df = spark.range(3)
    df = df.withColumn("uuid", F.laktory.uuid())
    '''
    +---+--------------------+
    | id|                uuid|
    +---+--------------------+
    |  0|acc0b53e-a36f-4f8...|
    |  1|56cdeb41-6828-486...|
    |  2|64a7d2bf-5e1d-41a...|
    +---+--------------------+
    '''
    ```
    """
    return F.expr("uuid()")
