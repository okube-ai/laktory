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
        y: Any = 0,
        where: STRING_OR_COLUMN = None,
        operator: str = "==",
        default: bool = None,
) -> Column:
    """
    Compare a column `x` and a value or another column `y` using
    `operator`. Comparison can be limited to `where` and assigned
    `default` elsewhere.

    output = `x` `operator` `y`

    Parameters
    ---------
    x : pd.Series
        Base series to compare
    y : pd.Series or Object
        Series or object to compare to
    where: pd.Series
        Where to apply the comparison
    operator: str
        Operator for comparison
    default: bool
        Default value to be applied outside of where

    Returns
    -------
    output: pd.Series
        Comparison result
    """

    x = _col(x)
    y = _col(y)
    y = _lit(y)

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
        c = F.when(where, c).otherwise(default)

    return c

