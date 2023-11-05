from typing import Union
import pyspark.sql.functions as F

# from pyspark.sql.functions import pandas_udf
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
    x: pyspark.sql.functions.column.Column or column name
        Input text series to split
    pattern: str or pyspark.sql.functions.column.Column
        String or regular expression to split on. If not specified, split on whitespace.
    key: int or pyspark.sql.functions.column.Column
        Split index to return

    Returns
    -------
    output: pd.Series
        Output series
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
    """
    return F.expr("uuid()")
