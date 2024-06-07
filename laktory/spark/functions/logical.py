import pyspark.sql.functions as F

from pyspark.sql.column import Column
from typing import Any
from laktory.spark.functions._common import (
    COLUMN_OR_NAME,
    INT_OR_COLUMN,
    FLOAT_OR_COLUMN,
    STRING_OR_COLUMN,
    _col,
    _lit,
)

__all__ = [
    "compare",
]


# --------------------------------------------------------------------------- #
# compare                                                                     #
# --------------------------------------------------------------------------- #


def compare(
    x: COLUMN_OR_NAME,
    y: COLUMN_OR_NAME = 0,
    where: COLUMN_OR_NAME = None,
    operator: str = "==",
    default: COLUMN_OR_NAME = None,
) -> Column:
    """
    Compare a column `x` and a value or another column `y` using
    `operator`. Comparison can be limited to `where` and assigned
    `default` elsewhere.

    output = `x` `operator` `y`

    Parameters
    ---------
    x :
        Base column to compare
    y :
        Column to compare to
    where:
        Where to apply the comparison
    operator: str
        Operator for comparison
    default:
        Default value to be applied when `where` is `False`

    Returns
    -------
    :
        Comparison result

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import pyspark.sql.functions as F

    df = spark.createDataFrame([[0.45], [0.55]], ["x"])
    df = df.withColumn(
        "y",
        F.laktory.compare(
            "x",
            F.lit(0.5),
            operator=">",
        ),
    )
    print(df.laktory.show_string())
    '''
    +----+-----+
    |   x|    y|
    +----+-----+
    |0.45|false|
    |0.55| true|
    +----+-----+
    '''
    ```
    """

    x = _col(x)
    y = _col(y)

    if operator == "==":
        c = x == y
    elif operator == "!=":
        c = x != y
    elif operator == "<":
        c = x < y
    elif operator == "<=":
        c = x <= y
    elif operator == ">":
        c = x > y
    elif operator == ">=":
        c = x >= y
    else:
        raise ValueError(f"Operator '{operator}' is not supported.")

    if where is not None:
        where = _col(where)
        if default is None:
            default = F.lit(None)
        else:
            default = _col(default)

        c = F.when(where, c).otherwise(default)

    return c
