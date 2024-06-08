from functools import wraps
import polars as pl

from laktory.polars.expressions.math import roundp
from laktory.polars.expressions.sql import sql_expr
from laktory.polars.expressions.sort import row_number
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

    @wraps(convert_units)
    def convert_units(*args, **kwargs):
        return convert_units(*_parse_args(args), **kwargs)

    @wraps(roundp)
    def roundp(*args, **kwargs):
        return roundp(*_parse_args(args), **kwargs)

    @wraps(row_number)
    def row_number(*args, **kwargs):
        return row_number(*_parse_args(args), **kwargs)

    @wraps(sql_expr)
    def sql_expr(*args, **kwargs):
        return sql_expr(*_parse_args(args), **kwargs)



# TODO: Enable?
# pl.expr.laktory = pl.Expr.laktory
