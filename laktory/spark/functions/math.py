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
    "roundp",
]


# --------------------------------------------------------------------------- #
# Rounding                                                                    #
# --------------------------------------------------------------------------- #


def roundp(
    x: COLUMN_OR_NAME,
    p: FLOAT_OR_COLUMN = 1.0,
) -> Column:
    """
    Evenly round to the given precision

    Parameters
    ------
    x:
        Input column
    p:
        Precision

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import pyspark.sql.functions as F

    df = spark.createDataFrame([[0.781], [13.0]], ["x"])
    df = df.withColumn("y", F.laktory.roundp("x", p=5))
    print(df.laktory.show_string())
    '''
    +-----+----+
    |    x|   y|
    +-----+----+
    |0.781| 0.0|
    | 13.0|15.0|
    +-----+----+
    '''

    df = df.withColumn("y", F.laktory.roundp("x", p=0.25))
    print(df.laktory.show_string())
    '''
    +-----+----+
    |    x|   y|
    +-----+----+
    |0.781|0.75|
    | 13.0|13.0|
    +-----+----+
    '''
    ```
    """
    # eps0 = 1.0e-16
    # precision = float(precision)
    # if precision < eps0:
    #     raise ValueError("Precision must be greater than 1.0e-16")
    return F.round(_col(x) / _lit(p)) * _lit(p)
