import pyspark.sql.functions as F

from pyspark.sql.column import Column
from laktory.spark.functions._common import (
    COLUMN_OR_NAME,
    INT_OR_COLUMN,
    FLOAT_OR_COLUMN,
    STRING_OR_COLUMN,
    _col,
    _lit,
)

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
    ------
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
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    import laktory.spark.functions as LF

    spark = SparkSession.builder.getOrCreate()

    df = spark.range(1).withColumn("x", F.lit("price_close"))
    df = df.withColumn("y", LF.string_split("x", pattern="_", key=1))
    print(df.show_string())
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

    Parameters
    ------

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column

    Examples
    --------
    ```py
    from pyspark.sql import SparkSession
    import laktory.spark.functions as LF

    spark = SparkSession.builder.getOrCreate()

    df = spark.range(3)
    df = df.withColumn("uuid", LF.uuid())
    print(df.show_string())
    '''
    +---+--------------------+
    | id|                uuid|
    +---+--------------------+
    |  0|e61c5887-52e8-488...|
    |  1|047282fe-f403-458...|
    |  2|14d11bfa-ef47-442...|
    +---+--------------------+
    '''
    ```

    """
    return F.expr("uuid()")


if __name__ == "__main__":
    # from pyspark.sql import SparkSession
    # import laktory.spark.functions as LF
    #
    # spark = SparkSession.builder.getOrCreate()
    #
    # df = spark.range(3)
    # df.select(LF.uuid()).show()

    from pyspark.sql import SparkSession
    import laktory.spark.functions as LF

    spark = SparkSession.builder.getOrCreate()

    df = spark.range(1).withColumn("x", F.lit("price_close"))
    df.select("x", LF.string_split("x", pattern="_", key=1).alias("y")).show()

    # df.select(LF.string_split(F.lit("price.close"), pattern=".", key=0)).show()
