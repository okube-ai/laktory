import polars as pl
from typing import Union

EXPR_OR_NAME = Union[pl.Expr, str]
"""polars expression or column name"""

INT_OR_EXPR = Union[int, pl.Expr, str]
"""int, polars expression or column name"""

FLOAT_OR_EXPR = Union[float, pl.Expr, str]
"""float, polars expression or column name"""

STRING_OR_EXPR = Union[str, pl.Expr]
"""string or polars expression"""
