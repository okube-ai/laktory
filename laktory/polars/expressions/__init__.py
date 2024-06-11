from functools import wraps
import polars as pl

from laktory.polars.expressions.datetime import current_timestamp
from laktory.polars.expressions.logical import compare
from laktory.polars.expressions.math import roundp

# from laktory.polars.expressions.sort import coalesce
from laktory.polars.expressions.sort import row_number
from laktory.polars.expressions.sql import sql_expr
from laktory.polars.expressions.string import string_split
from laktory.polars.expressions.string import uuid
from laktory.polars.expressions.units import convert_units


def _parse_args(args):
    """
    Polars expressions are designed to be chained to each other. When an
    extension expression is called, the calling expression is stored as
    `_expr` attribute. To support both chained operations as
    `pl.col("x").laktory.roundp(p=2.0)`
    and stand alone operations as
    `pl.Expr.laktory.roundp(pl.col("x"), p=2.0)`

    we check the value first argument received in an expression and return
    value._expr when appropriate.
    """
    if len(args) > 0 and isinstance(args[0], LaktoryExpression):
        args = list(args)
        args[0] = args[0]._expr
        return tuple(args)
    return args


@pl.api.register_expr_namespace("laktory")
class LaktoryExpression:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    # @wraps(coalesce)
    # def coalesce(*args, **kwargs):
    #     return coalesce(*_parse_args(args), **kwargs)

    @wraps(compare)
    def compare(*args, **kwargs):
        return compare(*_parse_args(args), **kwargs)

    @wraps(convert_units)
    def convert_units(*args, **kwargs):
        return convert_units(*_parse_args(args), **kwargs)

    @wraps(current_timestamp)
    def current_timestamp(*args, **kwargs):
        return current_timestamp(*_parse_args(args), **kwargs)

    @wraps(roundp)
    def roundp(*args, **kwargs):
        return roundp(*_parse_args(args), **kwargs)

    @wraps(row_number)
    def row_number(*args, **kwargs):
        return row_number(*_parse_args(args), **kwargs)

    @wraps(sql_expr)
    def sql_expr(*args, **kwargs):
        return sql_expr(*_parse_args(args), **kwargs)

    @wraps(string_split)
    def string_split(*args, **kwargs):
        return string_split(*_parse_args(args), **kwargs)

    @wraps(uuid)
    def uuid(*args, **kwargs):
        return uuid(*_parse_args(args), **kwargs)


pl.expr.laktory = pl.Expr.laktory
