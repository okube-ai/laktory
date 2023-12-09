from typing import Union
import pyspark.sql.functions as F
from pyspark.sql.column import Column


COLUMN_OR_NAME = Union[Column, str]
"""spark column or column name"""

INT_OR_COLUMN = Union[int, COLUMN_OR_NAME]
"""int, spark column or column name"""

FLOAT_OR_COLUMN = Union[float, COLUMN_OR_NAME]
"""float, spark column or column name"""

STRING_OR_COLUMN = Union[str, Column]
"""string or spark column"""


def _col(col: str) -> Column:
    if isinstance(col, Column):
        return col

    return F.col(col)


def _lit(col: str) -> Column:
    if isinstance(col, Column):
        return col

    return F.lit(col)
