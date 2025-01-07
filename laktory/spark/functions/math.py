import pyspark.sql.functions as F
from pyspark.sql.column import Column

from laktory.spark.functions._common import COLUMN_OR_NAME
from laktory.spark.functions._common import FLOAT_OR_COLUMN
from laktory.spark.functions._common import _col
from laktory.spark.functions._common import _lit

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
    import pyspark.sql.functions as F

    import laktory  # noqa: F401

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
